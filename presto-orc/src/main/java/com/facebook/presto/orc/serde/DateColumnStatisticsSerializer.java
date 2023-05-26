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
import com.facebook.presto.orc.metadata.statistics.DateColumnStatistics;
import com.facebook.presto.orc.metadata.statistics.DateStatistics;
import com.facebook.presto.orc.metadata.statistics.HiveBloomFilter;

public class DateColumnStatisticsSerializer extends Serializer<DateColumnStatistics>{

  @Override
  public void write(Kryo kryo, Output output, DateColumnStatistics object) {
    output.writeByte((byte)StatisticsKind.DATE.ordinal());
    long numValues = object.getNumberOfValues();
    HiveBloomFilter filter = object.getBloomFilter();
    DateStatistics ds = object.getDateStatistics();
    Integer max = ds.getMax(); // ???
    Integer min = ds.getMin();
    output.writeLong(numValues);
    if (min == null) {
      output.writeByte(0);
    } else {
      output.writeByte(1);
      output.writeInt(min.intValue());
    }
    if (max == null) {
      output.writeByte(0);
    } else {
      output.writeByte(1);
      output.writeInt(max.intValue());
    }
    kryo.writeObjectOrNull(output, filter, HiveBloomFilter.class);
  }

  @Override
  public DateColumnStatistics read(Kryo kryo, Input input,
      Class<? extends DateColumnStatistics> type) {
    long numValues = input.readLong();
    Integer min = null;
    Integer max = null;
    int isNull = input.readByte();
    if (isNull != 0) {
      min = Integer.valueOf(input.readInt());
    }
    isNull = input.readByte();
    if (isNull != 0) {
      max = Integer.valueOf(input.readInt());
    }
    HiveBloomFilter filter = kryo.readObjectOrNull(input, HiveBloomFilter.class);
    DateStatistics ds = new DateStatistics(min, max);
    return new DateColumnStatistics(numValues, filter, ds);
  }

}
