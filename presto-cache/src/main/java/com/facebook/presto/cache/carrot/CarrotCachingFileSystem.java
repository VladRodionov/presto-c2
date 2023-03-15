/*
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.facebook.presto.cache.carrot;


import java.io.IOException;
import java.lang.management.ManagementFactory;
import java.net.URI;
import java.util.Iterator;
import java.util.Map;
import java.util.concurrent.Callable;
import java.util.concurrent.ConcurrentHashMap;

import javax.annotation.concurrent.ThreadSafe;
import javax.management.MBeanServer;
import javax.management.ObjectName;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.Path;

import com.carrot.cache.Cache;
import com.carrot.cache.util.CarrotConfig;
import com.carrot.cache.util.Utils;
import com.facebook.airlift.log.Logger;
import com.facebook.presto.cache.CachingFileSystem;
import com.facebook.presto.cache.carrot.jmx.CarrotCacheJMXSink;
import com.facebook.presto.cache.carrot.util.Stats;
import com.facebook.presto.hive.HiveFileContext;
import com.facebook.presto.hive.filesystem.ExtendedFileSystem;
import com.google.common.annotations.VisibleForTesting;

@ThreadSafe
public class CarrotCachingFileSystem extends CachingFileSystem {
  private static Logger LOG = Logger.get(CarrotCachingFileSystem.class);
  private static Cache cache; // singleton
  // TODO: does not allow to change configuration dynamically
  private static ConcurrentHashMap<String, CarrotCachingFileSystem> cachedFS =
      new ConcurrentHashMap<>();

  private final boolean cacheValidationEnabled;
  private int dataPageSize;
  private int ioBufferSize;
  private int ioPoolSize;
  private URI remoteURI;
  private Stats stats = new Stats();

  volatile private boolean inited = false;

  public static CarrotCachingFileSystem get(ExtendedFileSystem dataTier, URI uri,
      boolean cacheValidationEnabled) throws IOException {
    checkJavaVersion();

    CarrotCachingFileSystem fs = cachedFS.get(dataTier.toString());
    if (fs == null) {
      synchronized (CarrotCachingFileSystem.class) {
        if (cachedFS.contains(dataTier.toString())) {
          return cachedFS.get(dataTier.toString());
        }
        fs = new CarrotCachingFileSystem(dataTier, uri, cacheValidationEnabled);
        cachedFS.put(dataTier.toString(), fs);
      }
    }
    return fs;
  }
  
  public static Cache getCache() {
    return cache;
  }
  
  public Stats getStats() {
    return stats;
  }
  
  private static void checkJavaVersion() throws IOException {
    if (Utils.getJavaVersion() < 11) {
      throw new IOException("Java 11+ is required to run Velociraptor cache.");
    }
  }

  private CarrotCachingFileSystem(ExtendedFileSystem dataTier, URI uri) {
    this(dataTier, uri, false);
  }

  private CarrotCachingFileSystem(ExtendedFileSystem dataTier, URI uri,
      boolean cacheValidationEnabled) {
    super(dataTier, uri);
    this.cacheValidationEnabled = cacheValidationEnabled;
  }

  @Override
  public void initialize(URI uri, Configuration configuration) throws IOException {
    try {
      if (inited) {
        return;
      }
      if (cache != null) {
        return;
      }
      synchronized (getClass()) {
        if (cache != null) {
          return;
        }
        CarrotConfig config = fromHadoopConfiguration(configuration);
        this.dataPageSize = (int) configuration.getLong(CarrotCacheConfig.DATA_PAGE_SIZE, 1 << 20);
        this.ioBufferSize = (int) configuration.getLong(CarrotCacheConfig.IO_BUFFER_SIZE, 2 << 20);
        this.ioPoolSize = (int) configuration.getLong(CarrotCacheConfig.IO_POOL_SIZE, 32);
        this.remoteURI = uri;
        CarrotCachingInputStream.initIOPools(this.ioPoolSize);
        
        loadOrCreateCache(config);
        boolean metricsEnabled = configuration.getBoolean("cache.carrot.metrics-enabled", true);
        if (metricsEnabled) {
          registerJMXMetrics(config);
        }
      }
      LOG.info("Initialized cache[%s]", cache.getName());
      this.inited = true;
    } catch (Throwable e) {
      LOG.error(e);
      throw new IOException(e);
    }
  }
  
  private void loadOrCreateCache(CarrotConfig config) throws IOException {
    try {
      cache = Cache.loadCache(CarrotCacheConfig.CACHE_NAME);
      if (cache != null) {
        LOG.info("Loaded cache[%s] from the path: %s", cache.getName(),
          config.getCacheRootDir(cache.getName()));
      }
    } catch (IOException e) {
      LOG.error(e.getMessage());
    }
    if (cache == null) {
      // Create new instance
      LOG.info("Creating new cache");
      cache = new Cache(CarrotCacheConfig.CACHE_NAME, config);
      LOG.info("Created new cache[%s]", cache.getName());
    }
    Runtime.getRuntime().addShutdownHook(new Thread(() -> {
      try {
        long start = System.currentTimeMillis();
        LOG.info("Shutting down cache[%s]", CarrotCacheConfig.CACHE_NAME);
        cache.shutdown();
        long end = System.currentTimeMillis();
        LOG.info("Shutting down cache[%s] done in %dms", CarrotCacheConfig.CACHE_NAME,
          (end - start));
      } catch (IOException e) {
        LOG.error(e);
      }
    }));
    LOG.info("Shutdown hook installed for cache[%s]", cache.getName());
  }
  
  private void registerJMXMetrics(CarrotConfig config) {
    String domainName = config.getJMXMetricsDomainName();
    String mtype = this.remoteURI.toString();
    mtype = mtype.replace(":", "-");
    mtype = mtype.replace("/", "");
    cache.registerJMXMetricsSink(domainName, mtype);
    registerGeneralJMXMetricsSink(domainName, mtype);
  }
  
  private void registerGeneralJMXMetricsSink(String domainName, String type) {
    MBeanServer mbs = ManagementFactory.getPlatformMBeanServer(); 
    ObjectName name;
    try {
      name = new ObjectName(String.format("%s:type=%s,name=Statistics",domainName, type));
      CarrotCacheJMXSink mbean = new CarrotCacheJMXSink(this);
      mbs.registerMBean(mbean, name); 
    } catch (Exception e) {
      LOG.error("Failed", e);
    }
  }
  
  private CarrotConfig fromHadoopConfiguration(Configuration configuration) {
    CarrotConfig config = CarrotConfig.getInstance();
    Iterator<Map.Entry<String, String>> it = configuration.iterator();
    while (it.hasNext()) {
      Map.Entry<String, String> entry = it.next();
      String name = entry.getKey();
      if (CarrotConfig.isCarrotPropertyName(name)) {
        config.setProperty(name, entry.getValue());
      }
    }
    return config;
  }
  
  @Override
  public FSDataInputStream openFile(Path path, HiveFileContext hiveFileContext) throws Exception {
    Callable<FSDataInputStream> remoteCall = new Callable<FSDataInputStream>() {
      @Override
      public FSDataInputStream call() throws Exception {
        return dataTier.openFile(path, hiveFileContext);
      }
    };
    if (hiveFileContext.isCacheable() && hiveFileContext.getFileSize().isPresent()) {
      long fileLength = hiveFileContext.getFileSize().getAsLong();
      long modificationTime = hiveFileContext.getModificationTime();
      FSDataInputStream cachingInputStream =
          new FSDataInputStream(new CarrotCachingInputStream(cache, path, remoteCall, 
              modificationTime, fileLength, dataPageSize, ioBufferSize, stats, 
              //TODO: make number pages configurable
              /*new ScanDetector(10, dataPageSize))*/ null));
      if (cacheValidationEnabled) {
        return new CarrotCacheValidatingInputStream(cachingInputStream,
            dataTier.openFile(path, hiveFileContext));
      }
      return cachingInputStream;
    }
    return remoteCall.call();
  }

  @VisibleForTesting
  static void dispose() {
    cache.dispose();
    cachedFS.clear();
  }

  public int getDataPageSize() {
    return this.dataPageSize;
  }

  public int getPrefetchBufferSize() {
    return this.ioBufferSize;
  }

  public Object getRemoteFSURI() {
    return this.remoteURI;
  }
}
