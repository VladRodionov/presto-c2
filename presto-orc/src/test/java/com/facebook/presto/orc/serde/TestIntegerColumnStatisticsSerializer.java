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

import org.testng.annotations.Test;

import com.esotericsoftware.kryo.kryo5.io.Input;
import com.esotericsoftware.kryo.kryo5.io.Output;
import com.facebook.presto.orc.metadata.statistics.HiveBloomFilter;
import com.facebook.presto.orc.metadata.statistics.IntegerColumnStatistics;
import com.facebook.presto.orc.metadata.statistics.IntegerStatistics;

public class TestIntegerColumnStatisticsSerializer extends TestSerializerBase{

  @Test
  public void testIntegerColumnStatisticsSerializer() {
    
    HiveBloomFilter filter = TestUtils.getBloomFilter();
    long numValues = 1000;
    IntegerStatistics bs = new IntegerStatistics(1000000L, 2000000L, 50000000L);
    IntegerColumnStatistics bcs = new IntegerColumnStatistics(numValues, filter, bs);
    
    Output out = new Output(1 << 16);
    kryo.writeObject(out, bcs);
    
    Input in = new Input(out.getBuffer());
    int type = in.readByte();
    assertEquals(StatisticsKind.INTEGER, StatisticsKind.values()[type]);
    IntegerColumnStatistics read = kryo.readObject(in, IntegerColumnStatistics.class);
    assertEquals(bcs.getNumberOfValues(), read.getNumberOfValues());
    assertEquals(bcs.getIntegerStatistics().getMin(), read.getIntegerStatistics().getMin());
    assertEquals(bcs.getIntegerStatistics().getMax(), read.getIntegerStatistics().getMax());

    TestUtils.equals(bcs.getBloomFilter(), read.getBloomFilter());
    
    // filter = null
    out.reset();
    bcs = new IntegerColumnStatistics(numValues, null, bs);
    kryo.writeObject(out, bcs);
    
    in = new Input(out.getBuffer());
    type = in.readByte();
    assertEquals(StatisticsKind.INTEGER, StatisticsKind.values()[type]);
    read = kryo.readObject(in, IntegerColumnStatistics.class);
    assertEquals(bcs.getNumberOfValues(), read.getNumberOfValues());
    assertEquals(bcs.getIntegerStatistics().getMin(), read.getIntegerStatistics().getMin());
    assertEquals(bcs.getIntegerStatistics().getMax(), read.getIntegerStatistics().getMax());
    TestUtils.equals(bcs.getBloomFilter(), read.getBloomFilter());
  }
}
