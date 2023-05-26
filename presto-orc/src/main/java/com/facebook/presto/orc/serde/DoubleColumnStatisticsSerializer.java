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
import com.facebook.presto.orc.metadata.statistics.DoubleColumnStatistics;
import com.facebook.presto.orc.metadata.statistics.DoubleStatistics;
import com.facebook.presto.orc.metadata.statistics.HiveBloomFilter;

public class DoubleColumnStatisticsSerializer extends Serializer<DoubleColumnStatistics>{

  @Override
  public void write(Kryo kryo, Output output, DoubleColumnStatistics object) {
    output.writeByte((byte)StatisticsKind.DOUBLE.ordinal());

    long numValues = object.getNumberOfValues();
    HiveBloomFilter filter = object.getBloomFilter();
    DoubleStatistics ds = object.getDoubleStatistics();
    Double min = ds.getMin();
    Double max = ds.getMax();
    output.writeLong(numValues);
    if (min == null) {
      output.writeByte(0);
    } else {
      output.writeByte(1);
      output.writeDouble(min.doubleValue());
    }
    if (max == null) {
      output.writeByte(0);
    } else {
      output.writeByte(1);
      output.writeDouble(max.doubleValue());
    }
    kryo.writeObjectOrNull(output, filter, HiveBloomFilter.class);
  }

  @Override
  public DoubleColumnStatistics read(Kryo kryo, Input input,
      Class<? extends DoubleColumnStatistics> type) {
    
    long numValues = input.readLong();
    boolean notNull = input.readByte() != (byte) 0;
    Double min = null, max = null;
    if (notNull) {
      min = Double.valueOf(input.readDouble());
    }
    notNull = input.readByte() != 0;
    if (notNull) {
      max = Double.valueOf(input.readDouble());
    }
    
    HiveBloomFilter filter = kryo.readObjectOrNull(input, HiveBloomFilter.class);
    DoubleStatistics ds = new DoubleStatistics(min, max);
    return new DoubleColumnStatistics(numValues, filter, ds);
  }

}
