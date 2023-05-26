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

import com.esotericsoftware.kryo.kryo5.Kryo;
import com.esotericsoftware.kryo.kryo5.Serializer;
import com.esotericsoftware.kryo.kryo5.io.Input;
import com.esotericsoftware.kryo.kryo5.io.Output;
import com.facebook.presto.orc.metadata.statistics.HiveBloomFilter;
import com.facebook.presto.orc.metadata.statistics.IntegerColumnStatistics;
import com.facebook.presto.orc.metadata.statistics.IntegerStatistics;

public class IntegerColumnStatisticsSerializer extends Serializer<IntegerColumnStatistics>{

  @Override
  public void write(Kryo kryo, Output output, IntegerColumnStatistics object) {
    output.writeByte((byte)StatisticsKind.INTEGER.ordinal());

    long numValues = object.getNumberOfValues();
    HiveBloomFilter filter = object.getBloomFilter();
    IntegerStatistics ds = object.getIntegerStatistics();
    Long min = ds.getMin();
    Long max = ds.getMax();
    Long sum = ds.getSum();
    output.writeLong(numValues);
    if (min == null) {
      output.writeByte(0);
    } else {
      output.writeByte(1);
      output.writeLong(min.longValue());
    }
    if (max == null) {
      output.writeByte(0);
    } else {
      output.writeByte(1);
      output.writeLong(max.longValue());
    }
    if (sum == null) {
      output.writeByte(0);
    } else {
      output.writeByte(1);
      output.writeLong(sum.longValue());
    }
    kryo.writeObjectOrNull(output, filter, HiveBloomFilter.class);
  }

  @Override
  public IntegerColumnStatistics read(Kryo kryo, Input input,
      Class<? extends IntegerColumnStatistics> type) {
    
    long numValues = input.readLong();
    boolean notNull = input.readByte() != (byte) 0;
    Long min = null, max = null, sum = null;
    if (notNull) {
      min = Long.valueOf(input.readLong());
    }
    notNull = input.readByte() != 0;
    if (notNull) {
      max = Long.valueOf(input.readLong());
    }
    notNull = input.readByte() != 0;
    if (notNull) {
      sum = Long.valueOf(input.readLong());
    }
    HiveBloomFilter filter = kryo.readObjectOrNull(input, HiveBloomFilter.class);
    IntegerStatistics ds = new IntegerStatistics(min, max, sum);
    return new IntegerColumnStatistics(numValues, filter, ds);
  }

}
