package com.facebook.presto.orc.serde;

import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertTrue;

import java.util.ArrayList;
import java.util.List;

import org.testng.annotations.Test;

import com.esotericsoftware.kryo.kryo5.io.Input;
import com.esotericsoftware.kryo.kryo5.io.Output;
import com.facebook.presto.orc.metadata.statistics.BinaryColumnStatistics;
import com.facebook.presto.orc.metadata.statistics.BinaryStatistics;
import com.facebook.presto.orc.metadata.statistics.ColumnStatistics;
import com.facebook.presto.orc.metadata.statistics.DateColumnStatistics;
import com.facebook.presto.orc.metadata.statistics.DateStatistics;
import com.facebook.presto.orc.metadata.statistics.HiveBloomFilter;
import com.facebook.presto.orc.metadata.statistics.MapColumnStatistics;
import com.facebook.presto.orc.metadata.statistics.MapStatistics;
import com.facebook.presto.orc.metadata.statistics.MapStatisticsEntry;
import com.facebook.presto.orc.proto.DwrfProto.KeyInfo;
import com.facebook.presto.orc.protobuf.ByteString;

public class TestMapColumnStatisticsSerializer extends TestSerializerBase {
  private static final KeyInfo INT_KEY = KeyInfo.newBuilder().setIntKey(1).build();
  private static final KeyInfo STRING_KEY = KeyInfo.newBuilder().setBytesKey(ByteString.copyFromUtf8("s1")).build();
  
  @Test
  public void testMapStatisticsEntrySerializer() {
    HiveBloomFilter filter = TestUtils.getBloomFilter();
    long numValues = 1000;
    // BinaryStatistics
    BinaryStatistics bs = new BinaryStatistics(1000000);
    BinaryColumnStatistics bcs = new BinaryColumnStatistics(numValues, filter, bs);
    
    DateStatistics dbs = new DateStatistics(1000000, 2000000);
    DateColumnStatistics dbcs = new DateColumnStatistics(numValues, filter, dbs);
    
    
    MapStatisticsEntry entry1 = new MapStatisticsEntry(INT_KEY, bcs);
    MapStatisticsEntry entry2 = new MapStatisticsEntry(STRING_KEY, dbcs);
    List<MapStatisticsEntry> list = new ArrayList<MapStatisticsEntry>();
    list.add(entry1);
    list.add(entry2);
    MapStatistics ms = new MapStatistics(list);
    
    MapColumnStatistics mapStats = new MapColumnStatistics(numValues, filter, ms);
    Output out = new Output(1 << 16);
    kryo.writeObject(out, mapStats);
    
    Input in = new Input(out.getBuffer());
    MapColumnStatistics read = (MapColumnStatistics)kryo.readObject(in, ColumnStatistics.class);

    assertEquals(mapStats.getNumberOfValues(), read.getNumberOfValues());
    assertTrue(TestUtils.equals(mapStats.getBloomFilter(), read.getBloomFilter()));
    
    MapStatistics readms = read.getMapStatistics();
   
    List<MapStatisticsEntry> readList = readms.getEntries(); 
    
    assertEquals(list.size(), readList.size());
    
    MapStatisticsEntry e1 = readList.get(0);
    MapStatisticsEntry e2 = readList.get(1);
    
    assertEquals(entry1.getKey(), e1.getKey());
    assertEquals(entry2.getKey(), e2.getKey());
    
    BinaryColumnStatistics bcs1 = (BinaryColumnStatistics) e1.getColumnStatistics();
    assertEquals(bcs.getNumberOfValues(), bcs1.getNumberOfValues());
    assertEquals(bcs.getBinaryStatistics().getSum(), bcs1.getBinaryStatistics().getSum());
    TestUtils.equals(bcs.getBloomFilter(), bcs1.getBloomFilter());
    
    
    DateColumnStatistics dbcs1 = (DateColumnStatistics) e2.getColumnStatistics();
    
    assertEquals(dbcs.getNumberOfValues(), dbcs1.getNumberOfValues());
    assertEquals(dbcs.getDateStatistics().getMax(), dbcs1.getDateStatistics().getMax());
    assertEquals(dbcs.getDateStatistics().getMin(), dbcs1.getDateStatistics().getMin());
    TestUtils.equals(dbcs.getBloomFilter(), dbcs1.getBloomFilter());

  }
}
