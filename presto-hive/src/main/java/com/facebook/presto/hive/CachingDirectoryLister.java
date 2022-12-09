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
import static com.google.common.hash.Hashing.md5;
import static java.nio.charset.StandardCharsets.UTF_8;

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.TimeUnit;

import javax.inject.Inject;

import org.apache.hadoop.fs.BlockLocation;
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
import com.esotericsoftware.kryo.kryo5.Kryo;
import com.esotericsoftware.kryo.kryo5.Serializer;
import com.esotericsoftware.kryo.kryo5.io.Input;
import com.esotericsoftware.kryo.kryo5.io.Output;
import com.facebook.airlift.log.Logger;
import com.facebook.presto.hive.filesystem.ExtendedFileSystem;
import com.facebook.presto.hive.metastore.Partition;
import com.facebook.presto.hive.metastore.Table;
import com.facebook.presto.spi.SchemaTableName;
import com.google.common.annotations.VisibleForTesting;
import com.google.common.cache.Cache;
import com.google.common.cache.CacheBuilder;
import com.google.common.cache.Weigher;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;

import io.airlift.units.Duration;

public class CachingDirectoryLister
        implements DirectoryLister
{
    
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

        List<HiveFileInfo> files = provider.get(path);
        if (files != null) {
            return files.iterator();
        }
        Iterator<HiveFileInfo> iterator = delegate.list(fileSystem, table, path, partition, namenodeStats, hiveDirectoryContext);
        if (hiveDirectoryContext.isCacheable() && cachedTableChecker.isCachedTable(table.getSchemaTableName())) {
          return cachingIterator(iterator, path);
        }
        return iterator;
    }

    private Iterator<HiveFileInfo> cachingIterator(Iterator<HiveFileInfo> iterator, Path path)
    {
        return new Iterator<HiveFileInfo>()
        {
            private final List<HiveFileInfo> files = new ArrayList<>();
            private boolean putDone = false;
            @Override
            public boolean hasNext()
            {
                boolean hasNext = iterator.hasNext();
                if (!hasNext && !putDone) {
                    provider.put(path, files);
                    putDone = true;
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
        long maxSize = hiveClientConfig.getFileStatusCacheMaxSize().toBytes();
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
      
      /**
       * Use md5 hashes for keys. Its 128 bit long (16 bytes) 
       * cryptographic hash. 
       */
      boolean useMD5hash = false;
      
      private CarrotCacheProvider(ObjectCache cache) {
        this.cache = cache;
      }
      
      void setExpireAfterWrite(long expire) {
        this.expire = expire;
      }
      
      void setUseMD5hash(boolean b) {
        this.useMD5hash = b;
      }
      
      static CarrotCacheProvider get(HiveClientConfig config) {
        Objects.requireNonNull(config.getCarrotCacheRootDir(), "Carrot cache root directory is null");
        String rootDir = config.getCarrotCacheRootDir().getPath();
        String type = config.getCarrotCacheTypeName();
        long maxSize = config.getFileStatusCacheMaxSize().toBytes();
        long expireAfterWrite = config.getFileStatusCacheExpireAfterWrite().toMillis();
        int dataSegmentSize = (int) config.getCarrotDataSegmentSize().toBytes();
        boolean useMD5hashForKeys = config.isCarrotHashForKeysEnabled();
        ObjectCache cache = null;
        try {
          cache = ObjectCache.loadCache(rootDir, CACHE_NAME);
          log.error("Loaded cache=%s from=%s name=%s", cache, rootDir, CACHE_NAME);
          if (cache == null) {
            Builder builder = new Builder(CACHE_NAME);
            builder = builder
                
                .withCacheMaximumSize(maxSize)
                .withCacheDataSegmentSize(dataSegmentSize)
                .withRecyclingSelector(LRCRecyclingSelector.class.getName())
                .withDataWriter(BaseDataWriter.class.getName())
                .withMemoryDataReader(BaseMemoryDataReader.class.getName())
                .withFileDataReader(BaseFileDataReader.class.getName())
                .withMainQueueIndexFormat(CompactBaseWithExpireIndexFormat.class.getName());
     
            if (type.equals("DISK")) {
              boolean admissionControllerEnabled = config.isCarrotAdmissionControllerEnabled();
              double ratio = config.getCarrotAdmissionControllerRatio();
              if (admissionControllerEnabled) {
              builder = builder
                  .withAdmissionController(AQBasedAdmissionController.class.getName())
                  .withAdmissionQueueStartSizeRatio(ratio);
              }
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
        
        Class<?> keyClass = null;
        Class<?> valueClass = ArrayList.class;
        if (!useMD5hashForKeys) {
          keyClass = Path.class;
        } else {
          keyClass = byte[].class;
        }
        
        cache.addKeyValueClasses(keyClass, valueClass);
        
        ObjectCache.SerdeInitializationListener listener = (x) -> {
        
          x.register(HiveFileInfo.class, new HiveFileInfoSerializer());
          x.register(BlockLocation.class);
          x.register(java.util.Optional.class);
        };
        cache.addSerdeInitializationListener(listener);
        // We do not need this for testing
        cache.addShutdownHook();
        if (config.isCarrotJMXMetricsEnabled()) {
          log.info("CarrotCacheProvider JMX enabled");
          String domainName = config.getCarrotJMXDomainName();
          cache.registerJMXMetricsSink(domainName);
        }
        CarrotCacheProvider provider = new CarrotCacheProvider(cache);
        provider.setExpireAfterWrite(expireAfterWrite);
        provider.setUseMD5hash(useMD5hashForKeys);
        return provider;
      }
      
      static public class HiveFileInfoSerializer extends Serializer<HiveFileInfo> {
        
        public HiveFileInfoSerializer () {
        }
        
        @Override
        public void write (Kryo kryo, Output output, HiveFileInfo obj) {
          Path path = obj.getPath();
          boolean isDirectory = obj.isDirectory();
          BlockLocation[] blockLocations = obj.getBlockLocations();
          long length = obj.getLength();
          long fileModifiedTime = obj.getFileModifiedTime();
          java.util.Optional<byte[]> extraFileInfo = obj.getExtraFileInfo();
          Map<String, String> customSplitInfo = obj.getCustomSplitInfo();
          
          output.writeString(path.toString());
          output.writeBoolean(isDirectory);
          kryo.writeObject(output, blockLocations);
          output.writeLong(length);
          output.writeLong(fileModifiedTime);
          kryo.writeObject(output, extraFileInfo);
          int size = customSplitInfo == null? -1: customSplitInfo.size();
          output.writeInt(size);
          if (size >= 0) {
            for (Map.Entry<String, String> entry: customSplitInfo.entrySet()) {
              output.writeString(entry.getKey());
              output.writeString(entry.getValue());
            }
          }
        }

        @Override
        public HiveFileInfo read(Kryo kryo, Input input, Class<? extends HiveFileInfo> type) {
          Path p = new Path(input.readString());
          boolean isDirectory = input.readBoolean();
          BlockLocation[] blockLocations = kryo.readObject(input, BlockLocation[].class);
          long length = input.readLong();
          long fileModifiedTime = input.readLong();
          java.util.Optional<byte[]> extraFileInfo = kryo.readObject(input,  java.util.Optional.class);
          Map<String, String> map = new HashMap<String, String>();
          ImmutableMap<String, String> imap = null;
          int size = input.readInt();
          if (size >=0) {
            for (int i = 0; i < size; i++) {
              String key = input.readString();
              String value = input.readString();
              map.put(key, value);
            }
            imap = ImmutableMap.copyOf(map);
          }
          return new HiveFileInfo(p, isDirectory, blockLocations, length, fileModifiedTime, extraFileInfo, imap);
        }
      }
      @Override
      public List<HiveFileInfo> get(Path p) {
        try {
          Object key = getKey(p);
          List<HiveFileInfo> result = (List<HiveFileInfo>) cache.get(key);
          log.error("CACHE GET key=%s result=%s", p.toString(), result);
          return result;
        } catch(Exception e) {
          log.error(e);
          return null;
        }
      }
      @Override
      public void put(Path p, List<HiveFileInfo> infos) {
        try {
          Object key = getKey(p);
          boolean result = cache.put(key, infos, System.currentTimeMillis() + expire);
          log.error("CACHE PUT key=%s result=%s", p.toString(), Boolean.toString(result));
        } catch (IOException e) {
          log.error(e);
        }
      }
      
      @SuppressWarnings("deprecation")
      private Object getKey(Path p) {
        return useMD5hash?  md5().hashString(p.toString(), UTF_8).asBytes(): p;
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

