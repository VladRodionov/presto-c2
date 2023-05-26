package com.facebook.presto.orc.serde;

import static org.testng.Assert.assertEquals;

import org.testng.annotations.Test;

import com.esotericsoftware.kryo.kryo5.io.Input;
import com.esotericsoftware.kryo.kryo5.io.Output;
import com.facebook.presto.orc.metadata.statistics.BinaryColumnStatistics;
import com.facebook.presto.orc.metadata.statistics.BinaryStatistics;
import com.facebook.presto.orc.metadata.statistics.HiveBloomFilter;
import com.facebook.presto.orc.metadata.statistics.MapStatisticsEntry;
import com.facebook.presto.orc.proto.DwrfProto.KeyInfo;
import com.facebook.presto.orc.protobuf.ByteString;

public class TestMapStatisticsEntrySerializer extends TestSerializerBase {
  private static final KeyInfo INT_KEY = KeyInfo.newBuilder().setIntKey(1).build();
  private static final KeyInfo STRING_KEY = KeyInfo.newBuilder().setBytesKey(ByteString.copyFromUtf8("s1")).build();
  
  @Test
  public void testMapStatisticsEntrySerializer() {
    HiveBloomFilter filter = TestUtils.getBloomFilter();
    long numValues = 1000;
    // BinaryStatistics
    BinaryStatistics bs = new BinaryStatistics(1000000);
    BinaryColumnStatistics bcs = new BinaryColumnStatistics(numValues, filter, bs);
    
    MapStatisticsEntry entry = new MapStatisticsEntry(INT_KEY, bcs);
    Output out = new Output(1 << 16);
    kryo.writeObject(out, entry);
    
    Input in = new Input(out.getBuffer());
    MapStatisticsEntry read = kryo.readObject(in, MapStatisticsEntry.class);
    
    KeyInfo keyInfo = read.getKey();
    BinaryColumnStatistics stats = (BinaryColumnStatistics)read.getColumnStatistics();
    
    assertEquals(INT_KEY, keyInfo);

    assertEquals(bcs.getNumberOfValues(), stats.getNumberOfValues());
    assertEquals(bcs.getBinaryStatistics().getSum(), stats.getBinaryStatistics().getSum());
    TestUtils.equals(bcs.getBloomFilter(), stats.getBloomFilter());
    
    out.reset();
    
    entry = new MapStatisticsEntry(STRING_KEY, bcs);
    kryo.writeObject(out, entry);
    
    in = new Input(out.getBuffer());
    read = kryo.readObject(in, MapStatisticsEntry.class);
    
    keyInfo = read.getKey();
    stats = (BinaryColumnStatistics)read.getColumnStatistics();
    
    assertEquals(STRING_KEY, keyInfo);

    assertEquals(bcs.getNumberOfValues(), stats.getNumberOfValues());
    assertEquals(bcs.getBinaryStatistics().getSum(), stats.getBinaryStatistics().getSum());
    TestUtils.equals(bcs.getBloomFilter(), stats.getBloomFilter());
    
  }
}
