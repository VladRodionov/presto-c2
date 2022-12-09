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
import java.net.URI;
import java.util.Iterator;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

import javax.annotation.concurrent.ThreadSafe;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.Path;

import com.carrot.cache.Cache;
import com.carrot.cache.util.CarrotConfig;
import com.carrot.cache.util.Utils;
import com.facebook.airlift.log.Logger;
import com.facebook.presto.cache.CachingFileSystem;
import com.facebook.presto.hive.HiveFileContext;
import com.facebook.presto.hive.filesystem.ExtendedFileSystem;
import com.google.common.annotations.VisibleForTesting;

@ThreadSafe
public class CarrotCachingFileSystem
        extends CachingFileSystem
{
    private static Logger LOG = Logger.get(CarrotCachingFileSystem.class);
    private static Cache cache; // singleton
    
    //TODO: does not allow to change configuration dynamically
    private static ConcurrentHashMap<String, CarrotCachingFileSystem> cachedFS =
        new ConcurrentHashMap<>();
    
    private final boolean cacheValidationEnabled;
    volatile private int dataPageSize;
    volatile private int ioBufferSize;
    volatile private int ioPoolSize;
    volatile private boolean inited = false;
    
    public static CarrotCachingFileSystem get(ExtendedFileSystem dataTier, 
        URI uri, boolean cacheValidationEnabled) throws IOException {
      checkJavaVersion();
      
      CarrotCachingFileSystem fs = cachedFS.get(dataTier.getScheme());
      if (fs == null) {
        synchronized(CarrotCachingFileSystem.class) {
          if (cachedFS.contains(dataTier.getScheme())) {
            return cachedFS.get(dataTier.getScheme());
          }
          fs = new CarrotCachingFileSystem(dataTier, uri, cacheValidationEnabled);
          cachedFS.put(dataTier.getScheme(), fs);
        }
      }
      return fs;
    }
    
    private static void checkJavaVersion() throws IOException{
      if (Utils.getJavaVersion() < 11) {
        throw new IOException("Java 11+ is required to run CARROT cache.");
      }
    }

    private CarrotCachingFileSystem(ExtendedFileSystem dataTier, URI uri)
    {
        this(dataTier, uri, false);
    }

    private CarrotCachingFileSystem(ExtendedFileSystem dataTier, URI uri, boolean cacheValidationEnabled)
    {
        super(dataTier, uri);
        this.cacheValidationEnabled = cacheValidationEnabled;   
    }

    @Override
    public void initialize(URI uri, Configuration configuration)
            throws IOException
    { 
      
      try {
      if (inited) {
        return;
      }

      CarrotConfig config = CarrotConfig.getInstance();
      Iterator<Map.Entry<String, String>> it = configuration.iterator();
      while (it.hasNext()) {
        Map.Entry<String, String> entry = it.next();
        String name = entry.getKey();
        if (CarrotConfig.isCarrotPropertyName(name)) {
          config.setProperty(name, entry.getValue());
        }
      }
      this.dataPageSize = (int) configuration.getLong("cache.carrot.data-page-size", 1024 * 1024);
      this.ioBufferSize = (int) configuration.getLong("cache.carrot.io-buffer-size", 256 * 1024);
      this.ioPoolSize = (int) configuration.getLong("cache.carrot.io-pool-size", 32);
      
      if (cache != null) {
        return;
      }

      synchronized (getClass()) {

        if (cache != null) {
          return;
        }

        CarrotCachingInputStream.initIOPools(this.ioPoolSize);
        try {
          
          cache = Cache.loadCache(CarrotCacheConfig.CACHE_NAME);
          if (cache != null) {
            LOG.info("Loaded cache[%s] from the path: %s\n", cache.getName(), 
              config.getCacheRootDir(cache.getName()));
          }
        } catch (IOException e) {
          LOG.error(e.getMessage());
        }
        
        if (cache == null) {
          // Create new instance
          LOG.info("Creating new cache");
          cache = new Cache(CarrotCacheConfig.CACHE_NAME, config);
          LOG.info("Created new cache[%s]\n", cache.getName());
        }
        
        Runtime.getRuntime().addShutdownHook(new Thread(() -> {
          try {
            long start = System.currentTimeMillis();
            LOG.info("Shutting down cache[%s]", CarrotCacheConfig.CACHE_NAME);
            cache.shutdown();
            long end = System.currentTimeMillis();
            LOG.info("Shutting down cache[%s] done in %dms", CarrotCacheConfig.CACHE_NAME, (end - start));
          } catch (IOException e) {
            LOG.error(e);
          }
        }));
        LOG.info("Shutdown hook installed for cache[%s]\n", cache.getName());

        
        boolean metricsEnabled = configuration.getBoolean("cache.carrot.metrics-enabled", true);

        if (metricsEnabled) {
          String domainName = config.getJMXMetricsDomainName();
          cache.registerJMXMetricsSink(domainName);

        }
      }
      LOG.info("Initialized cache[%s]\n", cache.getName());

      this.inited = true;
      } catch (Throwable e) {
        LOG.error(e);
        throw new IOException(e);
      }
    }

    @Override
    public FSDataInputStream openFile(Path path, HiveFileContext hiveFileContext) throws Exception {
      
      FSDataInputStream extStream = dataTier.openFile(path, hiveFileContext);
    
      if (hiveFileContext.isCacheable() && hiveFileContext.getFileSize().isPresent()) {

        long fileLength = hiveFileContext.getFileSize().getAsLong();
        
        FSDataInputStream cachingInputStream = new FSDataInputStream(
            new CarrotCachingInputStream(cache, path, extStream, fileLength, dataPageSize, ioBufferSize));
        if (cacheValidationEnabled) {
          return new CarrotCacheValidatingInputStream(cachingInputStream,
              dataTier.openFile(path, hiveFileContext));
        }

        return cachingInputStream;
      }
      return extStream;
    }
    
    @VisibleForTesting
    static void dispose() {
      cache.dispose();
      cachedFS.clear();
    }
}
