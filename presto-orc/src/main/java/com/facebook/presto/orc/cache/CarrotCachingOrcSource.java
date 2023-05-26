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
package com.facebook.presto.orc.cache;

import static com.facebook.presto.common.RuntimeUnit.BYTE;
import static com.facebook.presto.common.RuntimeUnit.NONE;
import static com.facebook.presto.orc.metadata.Stream.StreamKind.BLOOM_FILTER;
import static com.facebook.presto.orc.metadata.Stream.StreamKind.ROW_INDEX;
import static com.google.common.base.Throwables.throwIfInstanceOf;
import static java.lang.Math.toIntExact;
import static java.util.Objects.requireNonNull;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.Map.Entry;

import com.carrot.cache.Builder;
import com.carrot.cache.ObjectCache;
import com.carrot.cache.controllers.AQBasedAdmissionController;
import com.carrot.cache.controllers.LRCRecyclingSelector;
import com.carrot.cache.io.BaseDataWriter;
import com.carrot.cache.io.BaseFileDataReader;
import com.carrot.cache.io.BaseMemoryDataReader;
import com.facebook.presto.common.RuntimeStats;
import com.facebook.presto.orc.DiskRange;
import com.facebook.presto.orc.OrcDataSource;
import com.facebook.presto.orc.OrcDataSourceId;
import com.facebook.presto.orc.OrcDataSourceInput;
import com.facebook.presto.orc.OrcWriteValidation;
import com.facebook.presto.orc.StreamId;
import com.facebook.presto.orc.StripeMetadataSource;
import com.facebook.presto.orc.StripeReader.StripeId;
import com.facebook.presto.orc.StripeReader.StripeStreamId;
import com.facebook.presto.orc.metadata.DwrfStripeCacheData;
import com.facebook.presto.orc.metadata.MetadataReader;
import com.facebook.presto.orc.metadata.OrcFileTail;
import com.facebook.presto.orc.metadata.PostScript.HiveWriterVersion;
import com.facebook.presto.orc.metadata.Stream.StreamKind;
import com.facebook.presto.orc.metadata.RowGroupIndex;
import com.facebook.presto.orc.metadata.statistics.BinaryColumnStatistics;
import com.facebook.presto.orc.metadata.statistics.BooleanColumnStatistics;
import com.facebook.presto.orc.metadata.statistics.ColumnStatistics;
import com.facebook.presto.orc.metadata.statistics.DateColumnStatistics;
import com.facebook.presto.orc.metadata.statistics.DecimalColumnStatistics;
import com.facebook.presto.orc.metadata.statistics.DoubleColumnStatistics;
import com.facebook.presto.orc.metadata.statistics.HiveBloomFilter;
import com.facebook.presto.orc.metadata.statistics.IntegerColumnStatistics;
import com.facebook.presto.orc.metadata.statistics.MapColumnStatistics;
import com.facebook.presto.orc.metadata.statistics.MapStatisticsEntry;
import com.facebook.presto.orc.metadata.statistics.StringColumnStatistics;
import com.facebook.presto.orc.serde.BinaryColumnStatisticsSerializer;
import com.facebook.presto.orc.serde.BooleanColumnStatisticsSerializer;
import com.facebook.presto.orc.serde.ColumnStatisticsSerializer;
import com.facebook.presto.orc.serde.DateColumnStatisticsSerializer;
import com.facebook.presto.orc.serde.DecimalColumnStatisticsSerializer;
import com.facebook.presto.orc.serde.DoubleColumnStatisticsSerializer;
import com.facebook.presto.orc.serde.DwrfStripeCacheDataSerializer;
import com.facebook.presto.orc.serde.HiveBloomFilterSerializer;
import com.facebook.presto.orc.serde.IntegerColumnStatisticsSerializer;
import com.facebook.presto.orc.serde.MapColumnStatisticsSerializer;
import com.facebook.presto.orc.serde.MapStatisticsEntrySerializer;
import com.facebook.presto.orc.serde.OrcDataSourceIdSerializer;
import com.facebook.presto.orc.serde.OrcFileTailSerializer;
import com.facebook.presto.orc.serde.RowGroupIndexSerializer;
import com.facebook.presto.orc.serde.SliceSerializer;
import com.facebook.presto.orc.serde.StringColumnStatisticsSerializer;
import com.facebook.presto.orc.serde.StripeIdSerializer;
import com.facebook.presto.orc.serde.StripeStreamIdForIndexSerializer;
import com.facebook.presto.orc.serde.StripeStreamIdSerializer;
import com.facebook.presto.orc.stream.OrcInputStream;
import com.google.common.collect.ImmutableMap;
import com.google.common.util.concurrent.UncheckedExecutionException;

import io.airlift.slice.BasicSliceInput;
import io.airlift.slice.Slice;
import io.airlift.slice.Slices;
import io.airlift.units.DataSize;

public class CarrotCachingOrcSource implements OrcFileTailSource, StripeMetadataSource {

  public static class StripeStreamIdForIndex {

    private StripeStreamId parent;

    public StripeStreamIdForIndex(StripeStreamId id) {
      this.parent = id;
    }

    public String toString() {
      return parent.toString() + ":index";
    }
  }

  public static class CarrotCacheStatsMBean {
    private final ObjectCache cache;

    public CarrotCacheStatsMBean(ObjectCache cache) {
      this.cache = requireNonNull(cache, "cache is null");
    }

    public long getSize() {
      return cache.size();
    }

    public long getHitCount() {
      return cache.getTotalHits();
    }

    public long getMissCount() {
      return cache.getTotalGets() - cache.getTotalHits();
    }

    public double getHitRate() {
      return cache.getHitRate();
    }
  }

  private static CarrotCachingOrcSource instance;
  private final static String CACHE_OFFHEAP_NAME = "orc-metadata-offheap";
  private final static String CACHE_FILE_NAME = "orc-metadata-file";

  private OrcFileTailSource tailDelegate;

  private StripeMetadataSource stripeDelegate;

  private ObjectCache cache;

  public synchronized static CarrotCachingOrcSource getInstance(OrcCacheConfig config) {
    if (instance != null) {
      return instance;
    }

    instance = new CarrotCachingOrcSource(config);
    return instance;
  }

  private CarrotCachingOrcSource(OrcCacheConfig config) {
    cache = createOrLoadObjectCache(config);
  }

  public void setOrcFileTailSource(OrcFileTailSource delegate) {
    this.tailDelegate = delegate;
  }

  public void setStripeMetadataSource(StripeMetadataSource delegate) {
    this.stripeDelegate = delegate;
  }

  public CarrotCacheStatsMBean getStatsMBean() {
    return new CarrotCacheStatsMBean(cache);
  }

  private ObjectCache createOrLoadObjectCache(OrcCacheConfig config) {
    Objects.requireNonNull(config.getCarrotCacheRootDir(), "Carrot cache root directory is null");
    OrcMetadataCacheType cacheType = config.getMetadataCacheType();
    DataSize offheapCacheMaxSize = config.getCarrotOffheapMaxCacheSize();
    DataSize fileCacheMaxSize = config.getCarrotFileMaxCacheSize();
    if (cacheType == OrcMetadataCacheType.CARROT_HYBRID) {
      Objects.requireNonNull(offheapCacheMaxSize,
        "ORC metadata *carrot offheap* cache size is null");
      Objects.requireNonNull(fileCacheMaxSize, "ORC metadata *carrot file* cache size is null");
    } else if (cacheType == OrcMetadataCacheType.CARROT_FILE) {
      Objects.requireNonNull(fileCacheMaxSize, "ORC metadata file cache size is null");
    } else {
      Objects.requireNonNull(offheapCacheMaxSize, "ORC metadata offheap cache size is null");
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

  private ObjectCache createFileCache(OrcCacheConfig config) {
    String rootDir = config.getCarrotCacheRootDir().getPath();
    DataSize cacheMaxSize = config.getCarrotFileMaxCacheSize();
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

  private ObjectCache createOffheapCache(OrcCacheConfig config) {
    String rootDir = config.getCarrotCacheRootDir().getPath();
    DataSize cacheMaxSize = config.getCarrotOffheapMaxCacheSize();
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
            //TODO another index format - we do not need expiration support
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

  private ObjectCache createHybridCache(OrcCacheConfig config) {
    ObjectCache cache = createOffheapCache(config);
    ObjectCache fileCache = createFileCache(config);
    cache.getNativeCache().setVictimCache(fileCache.getNativeCache());
    return cache;
  }

  private void initSerDes(ObjectCache cache) {
    // For file tail cache
    cache.addKeyValueClasses(OrcDataSourceId.class, OrcFileTail.class);
    // For footer cache
    cache.addKeyValueClasses(StripeId.class, Slice.class);
    // For stream cache
    cache.addKeyValueClasses(StripeStreamId.class, Slice.class);
    // For index cache
    cache.addKeyValueClasses(StripeStreamIdForIndex.class, List.class);
    ObjectCache.SerdeInitializationListener listener = (x) -> {
      x.setRegistrationRequired(true);
      x.register(ArrayList.class);
      x.register(OrcDataSourceId.class, new OrcDataSourceIdSerializer());
      x.register(StripeId.class, new StripeIdSerializer());
      x.register(StripeStreamId.class, new StripeStreamIdSerializer());
      x.register(StripeStreamIdForIndex.class, new StripeStreamIdForIndexSerializer());
      x.register(Slice.class, new SliceSerializer());
      x.register(HiveBloomFilter.class, new HiveBloomFilterSerializer());
      x.register(RowGroupIndex.class, new RowGroupIndexSerializer());
      x.register(ColumnStatistics.class, new ColumnStatisticsSerializer());
      x.register(BooleanColumnStatistics.class, new BooleanColumnStatisticsSerializer());
      x.register(BinaryColumnStatistics.class, new BinaryColumnStatisticsSerializer());
      x.register(DateColumnStatistics.class, new DateColumnStatisticsSerializer());
      x.register(DecimalColumnStatistics.class, new DecimalColumnStatisticsSerializer());
      x.register(DoubleColumnStatistics.class, new DoubleColumnStatisticsSerializer());
      x.register(IntegerColumnStatistics.class, new IntegerColumnStatisticsSerializer());
      x.register(MapColumnStatistics.class, new MapColumnStatisticsSerializer());
      x.register(MapStatisticsEntry.class, new MapStatisticsEntrySerializer());
      x.register(StringColumnStatistics.class, new StringColumnStatisticsSerializer());
      x.register(OrcFileTail.class, new OrcFileTailSerializer());
      x.register(DwrfStripeCacheData.class, new DwrfStripeCacheDataSerializer());
    };
    cache.addSerdeInitializationListener(listener);

  }

  @Override
  public Slice getStripeFooterSlice(OrcDataSource orcDataSource, StripeId stripeId,
      long footerOffset, int footerLength, boolean cacheable) throws IOException {
    try {
      if (!cacheable) {
        return stripeDelegate.getStripeFooterSlice(orcDataSource, stripeId, footerOffset,
          footerLength, cacheable);
      }
      return (Slice) cache.get(stripeId, () -> stripeDelegate.getStripeFooterSlice(orcDataSource,
        stripeId, footerOffset, footerLength, cacheable));
    } catch (UncheckedExecutionException e) {
      throwIfInstanceOf(e.getCause(), IOException.class);
      throw new IOException("Unexpected error in stripe footer reading after footerSliceCache miss",
          e.getCause());
    }
  }

  @Override
  public Map<StreamId, OrcDataSourceInput> getInputs(OrcDataSource orcDataSource, StripeId stripeId,
      Map<StreamId, DiskRange> diskRanges, boolean cacheable) throws IOException {
    if (!cacheable) {
      return stripeDelegate.getInputs(orcDataSource, stripeId, diskRanges, cacheable);
    }

    // Fetch existing stream slice from cache
    ImmutableMap.Builder<StreamId, OrcDataSourceInput> inputsBuilder = ImmutableMap.builder();
    ImmutableMap.Builder<StreamId, DiskRange> uncachedDiskRangesBuilder = ImmutableMap.builder();
    for (Entry<StreamId, DiskRange> entry : diskRanges.entrySet()) {
      if (isCachedStream(entry.getKey().getStreamKind())) {
        Slice streamSlice = (Slice) cache.get(new StripeStreamId(stripeId, entry.getKey()));
        if (streamSlice != null) {
          inputsBuilder.put(entry.getKey(),
            new OrcDataSourceInput(new BasicSliceInput(streamSlice), streamSlice.length()));
        } else {
          uncachedDiskRangesBuilder.put(entry);
        }
      } else {
        uncachedDiskRangesBuilder.put(entry);
      }
    }

    // read ranges and update cache
    Map<StreamId, OrcDataSourceInput> uncachedInputs = stripeDelegate.getInputs(orcDataSource,
      stripeId, uncachedDiskRangesBuilder.build(), cacheable);
    for (Entry<StreamId, OrcDataSourceInput> entry : uncachedInputs.entrySet()) {
      if (isCachedStream(entry.getKey().getStreamKind())) {
        // We need to rewind the input after eagerly reading the slice.
        Slice streamSlice = Slices.wrappedBuffer(entry.getValue().getInput()
            .readSlice(toIntExact(entry.getValue().getInput().length())).getBytes());
        cache.put(new StripeStreamId(stripeId, entry.getKey()), streamSlice, 0);
        inputsBuilder.put(entry.getKey(), new OrcDataSourceInput(new BasicSliceInput(streamSlice),
            toIntExact(streamSlice.getRetainedSize())));
      } else {
        inputsBuilder.put(entry.getKey(), entry.getValue());
      }
    }
    return inputsBuilder.build();
  }

  @Override
  public List<RowGroupIndex> getRowIndexes(MetadataReader metadataReader,
      HiveWriterVersion hiveWriterVersion, StripeId stripeId, StreamId streamId,
      OrcInputStream inputStream, List<HiveBloomFilter> bloomFilters, RuntimeStats runtimeStats)
      throws IOException {

    StripeStreamId sid = new StripeStreamId(stripeId, streamId);
    StripeStreamIdForIndex key = new StripeStreamIdForIndex(sid);

    @SuppressWarnings("unchecked")
    List<RowGroupIndex> rowGroupIndices = (List<RowGroupIndex>) cache.get(key);
    if (rowGroupIndices != null) {
      runtimeStats.addMetricValue("OrcRowGroupIndexCacheHit", NONE, 1);
      runtimeStats.addMetricValue("OrcRowGroupIndexInMemoryBytesRead", BYTE,
        rowGroupIndices.stream().mapToLong(RowGroupIndex::getRetainedSizeInBytes).sum());
      return rowGroupIndices;
    } else {

      rowGroupIndices = stripeDelegate.getRowIndexes(metadataReader, hiveWriterVersion, stripeId,
        streamId, inputStream, bloomFilters, runtimeStats);
      cache.put(key, rowGroupIndices, 0);
    }
    return rowGroupIndices;
  }

  @Override
  public OrcFileTail getOrcFileTail(OrcDataSource orcDataSource, MetadataReader metadataReader,
      Optional<OrcWriteValidation> writeValidation, boolean cacheable) throws IOException {
    try {
      if (cacheable) {
        return (OrcFileTail) cache.get(orcDataSource.getId(), () -> tailDelegate
            .getOrcFileTail(orcDataSource, metadataReader, writeValidation, cacheable));
      }
      return tailDelegate.getOrcFileTail(orcDataSource, metadataReader, writeValidation, cacheable);
    } catch (UncheckedExecutionException e) {
      throwIfInstanceOf(e.getCause(), IOException.class);
      throw new IOException("Unexpected error in orc file tail reading after cache miss",
          e.getCause());
    }
  }

  private static boolean isCachedStream(StreamKind streamKind) {
    // BLOOM_FILTER and ROW_INDEX are on the critical path to generate a stripe. Other stream kinds
    // could be lazily read.
    //TODO: can we cache more?
    return streamKind == BLOOM_FILTER || streamKind == ROW_INDEX;
  }
}
