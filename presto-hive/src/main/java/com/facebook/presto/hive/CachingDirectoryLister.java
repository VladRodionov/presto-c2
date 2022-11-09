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
package com.facebook.presto.hive;

import static com.google.common.base.Preconditions.checkArgument;
import static com.google.common.collect.ImmutableSet.toImmutableSet;
import static java.util.Objects.requireNonNull;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.Objects;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.TimeUnit;

import javax.inject.Inject;

import org.apache.hadoop.fs.Path;
import org.weakref.jmx.Managed;

import com.carrot.cache.Builder;
import com.carrot.cache.ObjectCache;
import com.carrot.cache.controllers.AQBasedAdmissionController;
import com.carrot.cache.controllers.LRCRecyclingSelector;
import com.carrot.cache.index.CompactBaseWithExpireIndexFormat;
import com.carrot.cache.io.BaseDataWriter;
import com.carrot.cache.io.BaseFileDataReader;
import com.carrot.cache.io.BaseMemoryDataReader;
import com.facebook.airlift.log.Logger;
import com.facebook.presto.hive.filesystem.ExtendedFileSystem;
import com.facebook.presto.hive.metastore.Partition;
import com.facebook.presto.hive.metastore.Table;
import com.facebook.presto.spi.SchemaTableName;
import com.google.common.annotations.VisibleForTesting;
import com.google.common.cache.Cache;
import com.google.common.cache.CacheBuilder;
import com.google.common.cache.Weigher;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableSet;

import io.airlift.units.Duration;

public class CachingDirectoryLister
        implements DirectoryLister
{
    
    private static Logger log = Logger.get(CachingDirectoryLister.class);
    public static interface CacheProvider {
      
      public List<HiveFileInfo> get(Path p);
      
      public void put(Path p, List<HiveFileInfo> infos);

      public void flushCache();
      
      public Double getHitRate();

      public Double getMissRate();

      public long getHitCount();

      public long getMissCount();

      public long getRequestCount();
      
      public void dispose();
      
    }    
    
    private final CacheProvider provider;
    
    private final CachedTableChecker cachedTableChecker;

    protected final DirectoryLister delegate;

    @Inject
    public CachingDirectoryLister(@ForCachingDirectoryLister DirectoryLister delegate, HiveClientConfig hiveClientConfig)
    {
        
      List<String> tables = hiveClientConfig.getFileStatusCacheTables();
      this.delegate = requireNonNull(delegate, "delegate is null");
      this.provider = requireNonNull(getCacheProvider(hiveClientConfig), "Cache provider is null");
      this.cachedTableChecker = new CachedTableChecker(requireNonNull(tables, "tables is null"));
    }

    @VisibleForTesting
    public CachingDirectoryLister(DirectoryLister delegate, Duration expireAfterWrite, 
        long maxSize, List<String> tables)
    {
        this.delegate = requireNonNull(delegate, "delegate is null");
        Cache<Path, List<HiveFileInfo>> cache = CacheBuilder.newBuilder()
                .maximumWeight(maxSize)
                .weigher((Weigher<Path, List<HiveFileInfo>>) (key, value) -> value.size())
                .expireAfterWrite(expireAfterWrite.toMillis(), TimeUnit.MILLISECONDS)
                .recordStats()
                .build();
        this.provider = new GuavaCacheProvider(cache);
        this.cachedTableChecker = new CachedTableChecker(requireNonNull(tables, "tables is null"));
    }
    
    @Override
    public Iterator<HiveFileInfo> list(
            ExtendedFileSystem fileSystem,
            Table table,
            Path path,
            Optional<Partition> partition,
            NamenodeStats namenodeStats,
            HiveDirectoryContext hiveDirectoryContext)
    {
        /*DEBUG*/ log.error("LIST %s %s", table, path);

        List<HiveFileInfo> files = provider.get(path);
        if (files != null) {
            return files.iterator();
        }
        Iterator<HiveFileInfo> iterator = delegate.list(fileSystem, table, path, partition, namenodeStats, hiveDirectoryContext);
        if (hiveDirectoryContext.isCacheable() && cachedTableChecker.isCachedTable(table.getSchemaTableName())) {
          /*DEBUG*/ log.error("CACHING ITERATOR %s %s", table, path);  
          return cachingIterator(iterator, path);
        }
        /*DEBUG*/ log.error("ITERATOR %s %s cacheable=%s cachedTable=%s", table, path,
          hiveDirectoryContext.isCacheable(), cachedTableChecker.isCachedTable(table.getSchemaTableName()));
        return iterator;
    }

    private Iterator<HiveFileInfo> cachingIterator(Iterator<HiveFileInfo> iterator, Path path)
    {
        return new Iterator<HiveFileInfo>()
        {
            private final List<HiveFileInfo> files = new ArrayList<>();

            @Override
            public boolean hasNext()
            {
                boolean hasNext = iterator.hasNext();
                if (!hasNext) {
                    provider.put(path, ImmutableList.copyOf(files));
                }
                return hasNext;
            }

            @Override
            public HiveFileInfo next()
            {
                HiveFileInfo next = iterator.next();
                files.add(next);
                return next;
            }
        };
    }
    
    public CacheProvider getCacheProvider() {
      return this.provider;
    }
    
    @Managed
    public void flushCache()
    {
        provider.flushCache();
    }

    @Managed
    public Double getHitRate()
    {
        return provider.getHitRate();
    }

    @Managed
    public Double getMissRate()
    {
        return provider.getMissRate();
    }

    @Managed
    public long getHitCount()
    {
        return provider.getHitCount();
    }

    @Managed
    public long getMissCount()
    {
        return provider.getMissCount();
    }

    @Managed
    public long getRequestCount()
    {
        return provider.getRequestCount();
    }

    private static class CachedTableChecker
    {
        private final Set<SchemaTableName> cachedTableNames;
        private final boolean cacheAllTables;

        public CachedTableChecker(List<String> cachedTables)
        {
            cacheAllTables = cachedTables.contains("*");
            if (cacheAllTables) {
                checkArgument(cachedTables.size() == 1, "Only '*' is expected when caching all tables");
                this.cachedTableNames = ImmutableSet.of();
            }
            else {
                this.cachedTableNames = cachedTables.stream()
                        .map(SchemaTableName::valueOf)
                        .collect(toImmutableSet());
            }
        }

        public boolean isCachedTable(SchemaTableName schemaTableName)
        {
            return cacheAllTables || cachedTableNames.contains(schemaTableName);
        }
    }
    
    private CacheProvider getCacheProvider(HiveClientConfig config) {
      String typeName = config.getFileStatusCacheProviderTypeName();
      if (typeName.equals(GuavaCacheProvider.TYPE_NAME)) {
        return new GuavaCacheProvider(config);
      } else if (typeName.equals(CarrotCacheProvider.TYPE_NAME)) {
        return CarrotCacheProvider.get(config);
      }
      return null;
    }
    
    static class GuavaCacheProvider implements CachingDirectoryLister.CacheProvider {
      
      static final String TYPE_NAME = "GUAVA";
      
      private Cache<Path, List<HiveFileInfo>> cache;
      
      GuavaCacheProvider (Cache<Path, List<HiveFileInfo>> cache){
        this.cache = cache;
      }
      
      GuavaCacheProvider (HiveClientConfig hiveClientConfig){
        Duration expireAfterWrite = hiveClientConfig.getFileStatusCacheExpireAfterWrite();
        long maxSize = hiveClientConfig.getFileStatusCacheMaxSize();
        cache = CacheBuilder.newBuilder()
            .maximumWeight(maxSize)
            .weigher((Weigher<Path, List<HiveFileInfo>>) (key, value) -> value.size())
            .expireAfterWrite(expireAfterWrite.toMillis(), TimeUnit.MILLISECONDS)
            .recordStats()
            .build();
      }
      
      @Override
      public List<HiveFileInfo> get(Path p) {
        return cache.getIfPresent(p);
      }

      @Override
      public void put(Path p, List<HiveFileInfo> infos) {
        cache.put(p, infos);
      }

      @Override
      public void flushCache() {
        cache.invalidateAll();
      }

      @Override
      public Double getHitRate() {
        return cache.stats().hitRate();
      }

      @Override
      public Double getMissRate() {
        return cache.stats().missRate();
      }

      @Override
      public long getHitCount() {
        return cache.stats().hitCount();
      }

      @Override
      public long getMissCount() {
        return cache.stats().missCount();
      }

      @Override
      public long getRequestCount() {
        return cache.stats().requestCount();
      }

      @Override
      public void dispose() {
        // Do nothing
      }
    }
    @SuppressWarnings("unchecked")

    static class CarrotCacheProvider implements CachingDirectoryLister.CacheProvider {
      
      static Logger log = Logger.get(CarrotCacheProvider.class);
      
      static final String TYPE_NAME = "CARROT";
      
      static final String CACHE_NAME = "file_status";
      
      ObjectCache cache;
      
      long expire;
      
      private CarrotCacheProvider(ObjectCache cache) {
        this.cache = cache;
      }
      
      void setExpireAfterWrite(long expire) {
        this.expire = expire;
      }
      
      static CarrotCacheProvider get(HiveClientConfig config) {
        String rootDir = config.getCarrotCacheRootDir();
        Objects.requireNonNull(rootDir, "Carrot cache root directory is null");
        String type = config.getCarrotCacheTypeName();
        long maxSize = config.getFileStatusCacheMaxSize();
        long expireAfterWrite = config.getFileStatusCacheExpireAfterWrite().toMillis();
        
        ObjectCache cache = null;
        try {
          cache = ObjectCache.loadCache(rootDir, CACHE_NAME);
          
          if (cache == null) {
            
            Builder builder = new Builder(CACHE_NAME);
            
            builder = builder
                .withObjectCacheKeyClass(Path.class)
                .withObjectCacheValueClass(ArrayList.class)
                .withCacheMaximumSize(maxSize)
                .withRecyclingSelector(LRCRecyclingSelector.class.getName())
                .withDataWriter(BaseDataWriter.class.getName())
                .withMemoryDataReader(BaseMemoryDataReader.class.getName())
                .withFileDataReader(BaseFileDataReader.class.getName())
                .withMainQueueIndexFormat(CompactBaseWithExpireIndexFormat.class.getName());
            
            if (type.equals("DISK")) {
              builder = builder
                  .withCacheDataSegmentSize(128 * 1024 * 1024)
                  .withAdmissionController(AQBasedAdmissionController.class.getName());
              cache = builder.buildObjectDiskCache();
            } else if (type.equals("MEMORY")){
              cache = builder.buildObjectMemoryCache();
            } else {
              log.error("Unrecognized type %s for Carrot cache", type);
              return null;
            }
          }
        } catch (IOException e) {
          log.error(e);
          return null;
        }
  
        // We do not need this for testing
        cache.addShutdownHook();
        if (config.isCarrotJMXMetricsEnabled()) {
          String domainName = config.getCarrotJMXDomainName();
          cache.registerJMXMetricsSink(domainName);
        }
        CarrotCacheProvider provider = new CarrotCacheProvider(cache);
        provider.setExpireAfterWrite(expireAfterWrite);
        return provider;
      }
      
      @Override
      public List<HiveFileInfo> get(Path p) {
        try {
          List<HiveFileInfo> result = (List<HiveFileInfo>) cache.get(p);
          /*DEBUG*/log.error("GET %s result=%s", p, result);
          return result;
        } catch(IOException e) {
          log.error(e);
          return null;
        }
      }

      @Override
      public void put(Path p, List<HiveFileInfo> infos) {
        try {
          
          boolean result = cache.put(p, infos, System.currentTimeMillis() + expire);
          /*DEBUG*/log.error("PUT %s result=%s", p, Boolean.toString(result));

        } catch (IOException e) {
          log.error(e);
        }
      }

      @Override
      public void flushCache() {
        //no op
      }

      @Override
      public Double getHitRate() {
        return cache.getHitRate();
      }

      @Override
      public Double getMissRate() {
        return 1.d - getHitRate();
      }

      @Override
      public long getHitCount() {
        return cache.getTotalHits();
      }

      @Override
      public long getMissCount() {
        return cache.getTotalGets() - cache.getTotalHits();
      }

      @Override
      public long getRequestCount() {
        return cache.getTotalGets();
      }

      @Override
      public void dispose() {
        this.cache.getNativeCache().dispose();
      }
    }
}

