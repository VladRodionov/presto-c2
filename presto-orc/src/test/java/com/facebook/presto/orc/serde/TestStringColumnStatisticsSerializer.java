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
package com.facebook.presto.orc.serde;

import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertTrue;

import java.util.Arrays;
import java.util.Random;

import org.testng.annotations.Test;

import com.esotericsoftware.kryo.kryo5.io.Input;
import com.esotericsoftware.kryo.kryo5.io.Output;
import com.facebook.presto.orc.metadata.statistics.HiveBloomFilter;
import com.facebook.presto.orc.metadata.statistics.StringColumnStatistics;
import com.facebook.presto.orc.metadata.statistics.StringStatistics;

import io.airlift.slice.Slice;
import io.airlift.slice.Slices;

public class TestStringColumnStatisticsSerializer extends TestSerializerBase{

  @Test
  public void testStringColumnStatisticsSerializer() {
    
    HiveBloomFilter filter = TestUtils.getBloomFilter();
    long numValues = 1000;
    
    Random r = new Random();
    byte[] buf1 = new byte[100];
    byte[] buf2 = new byte[100];
    r.nextBytes(buf1);
    r.nextBytes(buf2);
    Slice min = Slices.wrappedBuffer(buf1);
    Slice max = Slices.wrappedBuffer(buf2);
    Slice tmp = min;
    if (min.compareTo(max) > 0) {
      min  = max;
      max  = tmp;
    }
    StringStatistics bs = new StringStatistics(min, max, 50000000L);
    StringColumnStatistics bcs = new StringColumnStatistics(numValues, filter, bs);
    
    Output out = new Output(1 << 16);
    kryo.writeObject(out, bcs);
    
    Input in = new Input(out.getBuffer());
    int type = in.readByte();
    assertEquals(StatisticsKind.STRING, StatisticsKind.values()[type]);
    StringColumnStatistics read = kryo.readObject(in, StringColumnStatistics.class);
    assertEquals(bcs.getNumberOfValues(), read.getNumberOfValues());
    byte[] $buf1 = read.getStringStatistics().getMin().getBytes();
    byte[] $buf2 = read.getStringStatistics().getMax().getBytes();
    
    assertTrue(Arrays.equals(buf1, $buf1));
    assertTrue(Arrays.equals(buf2, $buf2));

    TestUtils.equals(bcs.getBloomFilter(), read.getBloomFilter());
    
    // filter = null
    out.reset();
    bcs = new StringColumnStatistics(numValues, null, bs);
    kryo.writeObject(out, bcs);
    
    in = new Input(out.getBuffer());
    type = in.readByte();
    assertEquals(StatisticsKind.STRING, StatisticsKind.values()[type]);
    read = kryo.readObject(in, StringColumnStatistics.class);
    assertEquals(bcs.getNumberOfValues(), read.getNumberOfValues());
    $buf1 = read.getStringStatistics().getMin().getBytes();
    $buf2 = read.getStringStatistics().getMax().getBytes();
    
    assertTrue(Arrays.equals(buf1, $buf1));
    assertTrue(Arrays.equals(buf2, $buf2));

    TestUtils.equals(bcs.getBloomFilter(), read.getBloomFilter());
  }
}
