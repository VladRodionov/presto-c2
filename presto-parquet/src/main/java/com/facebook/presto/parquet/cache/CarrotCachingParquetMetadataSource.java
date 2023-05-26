
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
package com.facebook.presto.parquet.cache;

import static com.google.common.base.Throwables.throwIfInstanceOf;
import static java.util.Objects.requireNonNull;

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Objects;
import java.util.Optional;

import org.apache.parquet.column.Encoding;
import org.apache.parquet.column.EncodingStats;
import org.apache.parquet.column.statistics.BinaryStatistics;
import org.apache.parquet.column.statistics.BooleanStatistics;
import org.apache.parquet.column.statistics.DoubleStatistics;
import org.apache.parquet.column.statistics.FloatStatistics;
import org.apache.parquet.column.statistics.IntStatistics;
import org.apache.parquet.column.statistics.LongStatistics;
import org.apache.parquet.column.statistics.Statistics;
import org.apache.parquet.crypto.InternalFileDecryptor;
import org.apache.parquet.hadoop.metadata.BlockMetaData;
import org.apache.parquet.hadoop.metadata.ColumnChunkMetaData;
import org.apache.parquet.hadoop.metadata.ColumnPath;
import org.apache.parquet.hadoop.metadata.CompressionCodecName;
import org.apache.parquet.hadoop.metadata.FileMetaData;
import org.apache.parquet.hadoop.metadata.ParquetMetadata;
import org.apache.parquet.schema.ColumnOrder;
import org.apache.parquet.schema.GroupType;
import org.apache.parquet.schema.LogicalTypeAnnotation;
import org.apache.parquet.schema.LogicalTypeAnnotation.BsonLogicalTypeAnnotation;
import org.apache.parquet.schema.LogicalTypeAnnotation.DateLogicalTypeAnnotation;
import org.apache.parquet.schema.LogicalTypeAnnotation.DecimalLogicalTypeAnnotation;
import org.apache.parquet.schema.LogicalTypeAnnotation.EnumLogicalTypeAnnotation;
import org.apache.parquet.schema.LogicalTypeAnnotation.IntLogicalTypeAnnotation;
import org.apache.parquet.schema.LogicalTypeAnnotation.IntervalLogicalTypeAnnotation;
import org.apache.parquet.schema.LogicalTypeAnnotation.JsonLogicalTypeAnnotation;
import org.apache.parquet.schema.LogicalTypeAnnotation.ListLogicalTypeAnnotation;
import org.apache.parquet.schema.LogicalTypeAnnotation.MapKeyValueTypeAnnotation;
import org.apache.parquet.schema.LogicalTypeAnnotation.MapLogicalTypeAnnotation;
import org.apache.parquet.schema.LogicalTypeAnnotation.StringLogicalTypeAnnotation;
import org.apache.parquet.schema.LogicalTypeAnnotation.TimeLogicalTypeAnnotation;
import org.apache.parquet.schema.LogicalTypeAnnotation.TimestampLogicalTypeAnnotation;
import org.apache.parquet.schema.LogicalTypeAnnotation.UUIDLogicalTypeAnnotation;
import org.apache.parquet.schema.MessageType;
import org.apache.parquet.schema.PrimitiveType;
import org.apache.parquet.schema.PrimitiveType.PrimitiveTypeName;
import org.apache.parquet.schema.Type.ID;
import org.apache.parquet.schema.Type.Repetition;
import org.weakref.jmx.Managed;

import com.carrot.cache.Builder;
import com.carrot.cache.ObjectCache;
import com.carrot.cache.controllers.AQBasedAdmissionController;
import com.carrot.cache.controllers.LRCRecyclingSelector;
import com.carrot.cache.index.CompactBaseWithExpireIndexFormat;
import com.carrot.cache.io.BaseDataWriter;
import com.carrot.cache.io.BaseFileDataReader;
import com.carrot.cache.io.BaseMemoryDataReader;
import com.facebook.presto.parquet.ParquetDataSource;
import com.facebook.presto.parquet.serde.BlockMetaDataSerializer;
import com.facebook.presto.parquet.serde.ColumnChunkMetaDataSerializer;
import com.facebook.presto.parquet.serde.ColumnOrderSerializer;
import com.facebook.presto.parquet.serde.ColumnPathSerializer;
import com.facebook.presto.parquet.serde.CompressionCodecNameSerializer;
import com.facebook.presto.parquet.serde.EncodingSerializer;
import com.facebook.presto.parquet.serde.EncodingStatsSerializer;
import com.facebook.presto.parquet.serde.FileMetaDataSerializer;
import com.facebook.presto.parquet.serde.GroupTypeSerializer;
import com.facebook.presto.parquet.serde.IDSerializer;
import com.facebook.presto.parquet.serde.LogicalTypeAnnotationSerializer;
import com.facebook.presto.parquet.serde.MessageTypeSerializer;
import com.facebook.presto.parquet.serde.ParquetFileMetadataSerializer;
import com.facebook.presto.parquet.serde.ParquetMetadataSerializer;
import com.facebook.presto.parquet.serde.PrimitiveTypeNameSerializer;
import com.facebook.presto.parquet.serde.PrimitiveTypeSerializer;
import com.facebook.presto.parquet.serde.RepetitionSerializer;
import com.facebook.presto.parquet.serde.StatisticsSerializer;
import com.google.common.util.concurrent.UncheckedExecutionException;

import io.airlift.units.DataSize;

public class CarrotCachingParquetMetadataSource implements ParquetMetadataSource {
  public static class CarrotCacheStatsMBean
  {
      private final ObjectCache cache;

      public CarrotCacheStatsMBean(ObjectCache cache)
      {
          this.cache = requireNonNull(cache, "cache is null");
      }

      @Managed
      public long getSize()
      {
          return cache.size();
      }

      @Managed
      public long getHitCount()
      {
          return cache.getTotalHits();
      }

      @Managed
      public long getMissCount()
      {
          return cache.getTotalGets() - cache.getTotalHits();
      }

      @Managed
      public double getHitRate()
      {
          return cache.getHitRate();
      }
  }

  
  private final static String CACHE_OFFHEAP_NAME = "parquet-metadata-offheap";
  private final static String CACHE_FILE_NAME = "parquet-metadata-file";

  private final ObjectCache cache;
  private final ParquetMetadataSource delegate;

  public CarrotCachingParquetMetadataSource(ParquetCacheConfig parquetCacheConfig,
      ParquetMetadataSource delegate) {
    this.cache = createOrLoadObjectCache(parquetCacheConfig);
    this.delegate = requireNonNull(delegate, "delegate is null");
  }

  private ObjectCache createOrLoadObjectCache(ParquetCacheConfig config) {
    Objects.requireNonNull(config.getCarrotCacheRootDir(), "Carrot cache root directory is null");
    ParquetMetadataCacheType cacheType = config.getParquetMetadataCacheType();
    DataSize cacheMaxSize = config.getMetadataCacheSize();
    DataSize offheapCacheMaxSize = config.getCarrotOffheapCacheSize();
    DataSize fileCacheMaxSize = config.getCarrotFileCacheSize();
    if (cacheType == ParquetMetadataCacheType.CARROT_HYBRID) {
      Objects.requireNonNull(offheapCacheMaxSize,
        "Parquet metadata *carrot offheap* cache size is null");
      Objects.requireNonNull(fileCacheMaxSize, "Parquet metadata *carrot file* cache size is null");
    } else {
      Objects.requireNonNull(cacheMaxSize, "Parquet metadata cache size is null");
    }
    ObjectCache cache = null;
    switch (cacheType) {
      case CARROT_OFFHEAP:
        cache = createOffheapCache(config);
      case CARROT_FILE:
        cache = createFileCache(config);
        break;
      case CARROT_HYBRID:
        cache = createHybridCache(config);
      default:
        return null;
    }
    initSerDes(cache);
    
    if (config.getCarrotCacheSaveOnShutdown()) {
      cache.addShutdownHook();
    }
    if (config.isCarrotJMXMetricsEnabled()) {
      String domainName = config.getCarrotJMXDomainName();
      cache.registerJMXMetricsSink(domainName);
    }
    return cache;
  }

  private ObjectCache createFileCache(ParquetCacheConfig config) {
    String rootDir = config.getCarrotCacheRootDir().getPath();
    ParquetMetadataCacheType cacheType = config.getParquetMetadataCacheType();

    DataSize cacheMaxSize =
        cacheType == ParquetMetadataCacheType.CARROT_FILE ? config.getMetadataCacheSize()
            : config.getCarrotFileCacheSize();

    DataSize dataSegmentSize = config.getCarrotFileDataSegmentSize();
    ObjectCache cache = null;
    try {
      cache = ObjectCache.loadCache(rootDir, CACHE_FILE_NAME);
      if (cache == null) {
        Builder builder = new Builder(CACHE_FILE_NAME);
        builder = builder.withCacheMaximumSize(cacheMaxSize.toBytes())
            .withCacheDataSegmentSize(dataSegmentSize.toBytes())
            .withRecyclingSelector(LRCRecyclingSelector.class.getName())
            .withDataWriter(BaseDataWriter.class.getName())
            .withMemoryDataReader(BaseMemoryDataReader.class.getName())
            .withFileDataReader(BaseFileDataReader.class.getName());
            //.withMainQueueIndexFormat(CompactBaseWithExpireIndexFormat.class.getName());

        boolean admissionControllerEnabled = config.isCarrotAdmissionControllerEnabled();
        double ratio = config.getCarrotAdmissionControllerRatio();
        if (admissionControllerEnabled) {
          builder = builder.withAdmissionController(AQBasedAdmissionController.class.getName())
              .withAdmissionQueueStartSizeRatio(ratio);
        }
        cache = builder.buildObjectDiskCache();
      }
      return cache;
    } catch (IOException e) {
      throw new RuntimeException(e);
    }
  }

  private ObjectCache createOffheapCache(ParquetCacheConfig config) {
    String rootDir = config.getCarrotCacheRootDir().getPath();
    ParquetMetadataCacheType cacheType = config.getParquetMetadataCacheType();

    DataSize cacheMaxSize =
        cacheType == ParquetMetadataCacheType.CARROT_OFFHEAP ? config.getMetadataCacheSize()
            : config.getCarrotOffheapCacheSize();
    DataSize dataSegmentSize = config.getCarrotOffheapDataSegmentSize();
    ObjectCache cache = null;
    try {
      cache = ObjectCache.loadCache(rootDir, CACHE_OFFHEAP_NAME);
      if (cache == null) {
        Builder builder = new Builder(CACHE_OFFHEAP_NAME);
        builder = builder.withCacheMaximumSize(cacheMaxSize.toBytes())
            .withCacheDataSegmentSize(dataSegmentSize.toBytes())
            .withRecyclingSelector(LRCRecyclingSelector.class.getName())
            .withDataWriter(BaseDataWriter.class.getName())
            .withMemoryDataReader(BaseMemoryDataReader.class.getName())
            .withFileDataReader(BaseFileDataReader.class.getName());
            //.withMainQueueIndexFormat(CompactBaseWithExpireIndexFormat.class.getName());

        boolean admissionControllerEnabled = config.isCarrotAdmissionControllerEnabled();
        double ratio = config.getCarrotAdmissionControllerRatio();
        if (admissionControllerEnabled) {
          builder = builder.withAdmissionController(AQBasedAdmissionController.class.getName())
              .withAdmissionQueueStartSizeRatio(ratio);
        }
        cache = builder.buildObjectDiskCache();

      }
      return cache;
    } catch (IOException e) {
      throw new RuntimeException(e);
    }
  }

  private ObjectCache createHybridCache(ParquetCacheConfig config) {
    ObjectCache cache = createOffheapCache(config);
    ObjectCache fileCache = createFileCache(config);
    cache.getNativeCache().setVictimCache(fileCache.getNativeCache());
    return cache;
  }

  @Override
  public ParquetFileMetadata getParquetMetadata(ParquetDataSource parquetDataSource, long fileSize,
      boolean cacheable, long modificationTime, Optional<InternalFileDecryptor> fileDecryptor,
      boolean readMaskedValue) throws IOException {
    try {
      ParquetFileMetadata fileMetadata = null;
      if (cacheable) {
        fileMetadata = (ParquetFileMetadata) cache.get(parquetDataSource.getId().toString(),
              () -> delegate.getParquetMetadata(parquetDataSource, fileSize, cacheable,
                modificationTime, fileDecryptor, readMaskedValue));
        if (fileMetadata.getModificationTime() == modificationTime) {
          return fileMetadata;
        } else {
          cache.delete(parquetDataSource.getId());
        }
      }
      fileMetadata = delegate.getParquetMetadata(parquetDataSource, fileSize, cacheable, modificationTime,
        fileDecryptor, readMaskedValue);
      if (cacheable) {
        cache.put(parquetDataSource.getId().toString(), fileMetadata, 0);
      }
      return fileMetadata;
    } catch (UncheckedExecutionException e) {
      throwIfInstanceOf(e.getCause(), IOException.class);
      throw new IOException("Unexpected error in parquet metadata reading after cache miss",
          e.getCause());
    }
  }
  
  public CarrotCacheStatsMBean getStatsMBean() {
    return new CarrotCacheStatsMBean(cache);
  }
  
  private void initSerDes(ObjectCache cache) {
    cache.addKeyValueClasses(String.class, ParquetFileMetadata.class);
    ObjectCache.SerdeInitializationListener listener = (x) -> {
      
     
      // Level 0
      x.register(ParquetFileMetadata.class, new ParquetFileMetadataSerializer());
      // Level 1
      x.register(ParquetMetadata.class, new ParquetMetadataSerializer());
      // Level 2
      x.register(FileMetaData.class, new FileMetaDataSerializer());
      x.register(ArrayList.class);
      x.register(BlockMetaData.class, new BlockMetaDataSerializer());
      // Level 3 - FileMetaData
      x.register(HashMap.class);
      x.register(MessageType.class, new MessageTypeSerializer());
      // We skip InternalFileDecryptor - it is null according to the source code of MetadataReader
      // Level 3 BlockMetaData
      x.register(ColumnChunkMetaData.class, new ColumnChunkMetaDataSerializer());
      
      try {
        Class<?> intMetaDataClass = Class.forName("org.apache.parquet.hadoop.metadata.IntColumnChunkMetaData");
        Class<?> longMetaDataClass = Class.forName("org.apache.parquet.hadoop.metadata.LongColumnChunkMetaData");
        x.register(intMetaDataClass, new ColumnChunkMetaDataSerializer());
        x.register(longMetaDataClass, new ColumnChunkMetaDataSerializer());
      } catch (ClassNotFoundException e) {
        throw new RuntimeException(e);
      }
      
      // Level 4: MessageType - GroupType
      x.register(GroupType.class, new GroupTypeSerializer());
      // Level 4: ColumnChunkMetaData -
      x.register(ColumnPath.class, new ColumnPathSerializer()); 
      x.register(PrimitiveType.class, new PrimitiveTypeSerializer()); 
      x.register(CompressionCodecName.class, new CompressionCodecNameSerializer()); 
      x.register(EncodingStats.class, new EncodingStatsSerializer());
      x.register(Encoding.class, new EncodingSerializer()); 
      x.register(Statistics.class, new StatisticsSerializer()); 
      x.register(IntStatistics.class, new StatisticsSerializer());
      x.register(LongStatistics.class, new StatisticsSerializer());
      x.register(FloatStatistics.class, new StatisticsSerializer());
      x.register(DoubleStatistics.class, new StatisticsSerializer());
      x.register(BooleanStatistics.class, new StatisticsSerializer());
      x.register(BinaryStatistics.class, new StatisticsSerializer());

      x.register(Repetition.class, new RepetitionSerializer());
      x.register(PrimitiveTypeName.class, new PrimitiveTypeNameSerializer());
      
      x.register(LogicalTypeAnnotation.class, new LogicalTypeAnnotationSerializer());
      x.register(BsonLogicalTypeAnnotation.class, new LogicalTypeAnnotationSerializer());
      x.register(DateLogicalTypeAnnotation.class, new LogicalTypeAnnotationSerializer());
      x.register(DecimalLogicalTypeAnnotation.class, new LogicalTypeAnnotationSerializer());
      x.register(EnumLogicalTypeAnnotation.class, new LogicalTypeAnnotationSerializer());
      x.register(IntervalLogicalTypeAnnotation.class, new LogicalTypeAnnotationSerializer());
      x.register(IntLogicalTypeAnnotation.class, new LogicalTypeAnnotationSerializer());
      x.register(JsonLogicalTypeAnnotation.class, new LogicalTypeAnnotationSerializer());
      x.register(ListLogicalTypeAnnotation.class, new LogicalTypeAnnotationSerializer());
      x.register(MapKeyValueTypeAnnotation.class, new LogicalTypeAnnotationSerializer());
      x.register(MapLogicalTypeAnnotation.class, new LogicalTypeAnnotationSerializer());
      x.register(StringLogicalTypeAnnotation.class, new LogicalTypeAnnotationSerializer());
      x.register(TimeLogicalTypeAnnotation.class, new LogicalTypeAnnotationSerializer());
      x.register(TimestampLogicalTypeAnnotation.class, new LogicalTypeAnnotationSerializer());
      x.register(UUIDLogicalTypeAnnotation.class, new LogicalTypeAnnotationSerializer());


      x.register(ID.class, new IDSerializer());
      x.register(ColumnOrder.class, new ColumnOrderSerializer());
      
    };
    cache.addSerdeInitializationListener(listener);
    // We do not need this for testing

  }
}


