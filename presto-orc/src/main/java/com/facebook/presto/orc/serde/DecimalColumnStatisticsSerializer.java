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

import java.math.BigDecimal;

import com.esotericsoftware.kryo.kryo5.Kryo;
import com.esotericsoftware.kryo.kryo5.Serializer;
import com.esotericsoftware.kryo.kryo5.io.Input;
import com.esotericsoftware.kryo.kryo5.io.Output;
import com.facebook.presto.orc.metadata.statistics.DecimalColumnStatistics;
import com.facebook.presto.orc.metadata.statistics.DecimalStatistics;
import com.facebook.presto.orc.metadata.statistics.HiveBloomFilter;

public class DecimalColumnStatisticsSerializer extends Serializer<DecimalColumnStatistics>{

  @Override
  public void write(Kryo kryo, Output output, DecimalColumnStatistics object) {
    output.writeByte((byte)StatisticsKind.DECIMAL.ordinal());

    long numValues = object.getNumberOfValues();
    HiveBloomFilter filter = object.getBloomFilter();
    DecimalStatistics ds = object.getDecimalStatistics();
    BigDecimal min = ds.getMin();
    BigDecimal max = ds.getMax();
    long size = ds.getDecimalSizeInBytes();

    output.writeLong(numValues);
    boolean isNull = min == null;
    if (isNull) {
      output.writeByte(0);
    } else {
      output.writeByte(1);
      output.writeString(min.toString());
    }
    isNull = max == null;
    if (isNull) {
      output.writeByte(0);
    } else {
      output.writeByte(1);
      output.writeString(max.toString());
    }
    output.writeLong(size);
    kryo.writeObjectOrNull(output, filter, HiveBloomFilter.class);
  }

  @Override
  public DecimalColumnStatistics read(Kryo kryo, Input input,
      Class<? extends DecimalColumnStatistics> type) {
    
    long numValues = input.readLong();
    BigDecimal min = null, max  = null;
    boolean notNull = input.readByte() != 0;
    if (notNull) {
      String v = input.readString();
      min = new BigDecimal(v);
    }
    notNull = input.readByte() != 0;
    if (notNull) {
      String v = input.readString();
      max = new BigDecimal(v);
    }
    long size = input.readLong();
    HiveBloomFilter filter = kryo.readObjectOrNull(input, HiveBloomFilter.class);
    return new DecimalColumnStatistics(numValues, filter, new DecimalStatistics(min, max, size));
  }

}
