package com.facebook.presto.orc.serde;

import java.util.ArrayList;

import org.testng.annotations.BeforeClass;

import com.esotericsoftware.kryo.kryo5.Kryo;
import com.facebook.presto.orc.OrcDataSourceId;
import com.facebook.presto.orc.StripeReader.StripeId;
import com.facebook.presto.orc.StripeReader.StripeStreamId;
import com.facebook.presto.orc.cache.CarrotCachingOrcSource.StripeStreamIdForIndex;
import com.facebook.presto.orc.metadata.DwrfStripeCacheData;
import com.facebook.presto.orc.metadata.OrcFileTail;
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

import io.airlift.slice.Slice;

public class TestSerializerBase {

  static Kryo kryo;

  @BeforeClass
  public static void setUp() {
    kryo = new Kryo();
    kryo.setRegistrationRequired(true);
    kryo.register(ArrayList.class);
    kryo.register(OrcDataSourceId.class, new OrcDataSourceIdSerializer());
    kryo.register(StripeId.class, new StripeIdSerializer());
    kryo.register(StripeStreamId.class, new StripeStreamIdSerializer());
    kryo.register(StripeStreamIdForIndex.class, new StripeStreamIdForIndexSerializer());
    kryo.register(Slice.class, new SliceSerializer());
    kryo.register(HiveBloomFilter.class, new HiveBloomFilterSerializer());
    kryo.register(RowGroupIndex.class, new RowGroupIndexSerializer());
    kryo.register(ColumnStatistics.class, new ColumnStatisticsSerializer());
    kryo.register(BooleanColumnStatistics.class, new BooleanColumnStatisticsSerializer());
    kryo.register(BinaryColumnStatistics.class, new BinaryColumnStatisticsSerializer());
    kryo.register(DateColumnStatistics.class, new DateColumnStatisticsSerializer());
    kryo.register(DecimalColumnStatistics.class, new DecimalColumnStatisticsSerializer());
    kryo.register(DoubleColumnStatistics.class, new DoubleColumnStatisticsSerializer());
    kryo.register(IntegerColumnStatistics.class, new IntegerColumnStatisticsSerializer());
    kryo.register(MapColumnStatistics.class, new MapColumnStatisticsSerializer());
    kryo.register(MapStatisticsEntry.class, new MapStatisticsEntrySerializer());
    kryo.register(StringColumnStatistics.class, new StringColumnStatisticsSerializer());
    kryo.register(OrcFileTail.class, new OrcFileTailSerializer());
    kryo.register(DwrfStripeCacheData.class, new DwrfStripeCacheDataSerializer());
  }
}
