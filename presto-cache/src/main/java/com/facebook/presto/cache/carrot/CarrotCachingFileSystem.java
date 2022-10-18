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

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.Path;

import com.carrot.cache.Cache;
import com.carrot.cache.util.CarrotConfig;
import com.facebook.presto.cache.CachingFileSystem;
import com.facebook.presto.hive.HiveFileContext;
import com.facebook.presto.hive.filesystem.ExtendedFileSystem;
import com.google.common.annotations.VisibleForTesting;

public class CarrotCachingFileSystem
        extends CachingFileSystem
{
    private final boolean cacheValidationEnabled;
    private Cache cache;
    private int dataPageSize;
    private int ioBufferSize;
    private int ioPoolSize;

    public CarrotCachingFileSystem(ExtendedFileSystem dataTier, URI uri)
    {
        this(dataTier, uri, false);
    }

    public CarrotCachingFileSystem(ExtendedFileSystem dataTier, URI uri, boolean cacheValidationEnabled)
    {
        super(dataTier, uri);
        this.cacheValidationEnabled = cacheValidationEnabled;
    }

    @Override
    public synchronized void initialize(URI uri, Configuration configuration)
            throws IOException
    { 
      
      CarrotConfig config = CarrotConfig.getInstance(); 
      Iterator<Map.Entry<String, String>> it = configuration.iterator();
      while(it.hasNext()) {
        Map.Entry<String, String> entry = it.next();
        String name = entry.getKey();
        if (CarrotConfig.isCarrotPropertyName(name)) {
          config.setProperty(name, entry.getValue());
        }
      }
      this.dataPageSize = (int) configuration.getLong("cache.carrot.data-page-size", 1024 * 1024);
      this.ioBufferSize = (int) configuration.getLong("cache.carrot.io-buffer-size", 256 * 1024);
      this.ioPoolSize = (int) configuration.getLong("cache.carrot.io-pool-size", 32);
      
      CarrotCachingInputStream.initIOPools(this.ioPoolSize);
      
      this.cache = new Cache(CarrotCacheConfig.CACHE_NAME, config);
      boolean metricsEnabled = configuration.getBoolean("cache.carrot.metrics-enabled", false);
      if (metricsEnabled) {
        String domainName = config.getJMXMetricsDomainName();
        this.cache.registerJMXMetricsSink(domainName);
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
    Cache getCache() {
      return this.cache;
    }
}
