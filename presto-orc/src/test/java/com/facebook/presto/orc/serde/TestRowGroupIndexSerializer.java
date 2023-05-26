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

import org.testng.annotations.Test;

import com.esotericsoftware.kryo.kryo5.io.Input;
import com.esotericsoftware.kryo.kryo5.io.Output;
import com.facebook.presto.orc.metadata.RowGroupIndex;
import com.facebook.presto.orc.metadata.statistics.BinaryColumnStatistics;
import com.facebook.presto.orc.metadata.statistics.BinaryStatistics;
import com.facebook.presto.orc.metadata.statistics.BooleanColumnStatistics;
import com.facebook.presto.orc.metadata.statistics.BooleanStatistics;
import com.facebook.presto.orc.metadata.statistics.DateColumnStatistics;
import com.facebook.presto.orc.metadata.statistics.DateStatistics;
import com.facebook.presto.orc.metadata.statistics.HiveBloomFilter;

public class TestRowGroupIndexSerializer extends TestSerializerBase {

  @Test
  public void testRowGroupIndexSerializer() {
    
    int[] positions = new int[] {0, 1, 2, 3, 4, 5, 6, 7, 8, 9};
    
    // 1 BinaryColumnStatistics
    HiveBloomFilter filter = TestUtils.getBloomFilter();
    long numValues = 1000;
    BinaryStatistics bs = new BinaryStatistics(1000000);
    BinaryColumnStatistics bcs = new BinaryColumnStatistics(numValues, filter, bs);
    
    RowGroupIndex groupIndex = new RowGroupIndex(positions, bcs);
    Output out = new Output(1 << 16);
    kryo.writeObject(out, groupIndex);
    
    Input in = new Input(out.getBuffer());
    
    RowGroupIndex read = kryo.readObject(in, RowGroupIndex.class);
    
    assertTrue(Arrays.equals(positions, read.getPositions()));
    
    BinaryColumnStatistics $bcs = (BinaryColumnStatistics)read.getColumnStatistics();
    assertEquals(bcs.getNumberOfValues(), $bcs.getNumberOfValues());
    assertEquals(bcs.getBinaryStatistics().getSum(), $bcs.getBinaryStatistics().getSum());
    TestUtils.equals(bcs.getBloomFilter(), $bcs.getBloomFilter());
    
    // 2 BooleanColumnStatistics
    filter = TestUtils.getBloomFilter();
    BooleanStatistics bools = new BooleanStatistics(1000000);
    BooleanColumnStatistics boolcs = new BooleanColumnStatistics(numValues, filter, bools);
    
    groupIndex = new RowGroupIndex(positions, boolcs);
    out = new Output(1 << 16);
    kryo.writeObject(out, groupIndex);
    
    in = new Input(out.getBuffer());
    
    read = kryo.readObject(in, RowGroupIndex.class);
    
    assertTrue(Arrays.equals(positions, read.getPositions()));
    
    BooleanColumnStatistics $boolcs = (BooleanColumnStatistics)read.getColumnStatistics();
    assertEquals(boolcs.getNumberOfValues(), $boolcs.getNumberOfValues());
    assertEquals(boolcs.getBooleanStatistics().getTrueValueCount(), $boolcs.getBooleanStatistics().getTrueValueCount());
    TestUtils.equals(boolcs.getBloomFilter(), $boolcs.getBloomFilter());
    
    // 3 DateColumnStatistics
    DateStatistics dates = new DateStatistics(1000000, 2000000);
    DateColumnStatistics datecs = new DateColumnStatistics(numValues, filter, dates);
    groupIndex = new RowGroupIndex(positions, datecs);

    out = new Output(1 << 16);
    kryo.writeObject(out, groupIndex);
    
    in = new Input(out.getBuffer());
    
    read = kryo.readObject(in, RowGroupIndex.class);
    assertTrue(Arrays.equals(positions, read.getPositions()));
    DateColumnStatistics dcs = (DateColumnStatistics) read.getColumnStatistics();

    assertEquals(datecs.getNumberOfValues(), dcs.getNumberOfValues());
    assertEquals(datecs.getDateStatistics().getMin(), dcs.getDateStatistics().getMin());
    assertEquals(datecs.getDateStatistics().getMax(), dcs.getDateStatistics().getMax());

    TestUtils.equals(datecs.getBloomFilter(), dcs.getBloomFilter());
    
  }
}
