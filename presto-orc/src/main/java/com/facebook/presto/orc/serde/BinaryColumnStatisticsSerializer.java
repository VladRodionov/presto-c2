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
import com.facebook.presto.orc.metadata.statistics.BinaryColumnStatistics;
import com.facebook.presto.orc.metadata.statistics.BinaryStatistics;
import com.facebook.presto.orc.metadata.statistics.HiveBloomFilter;

public class BinaryColumnStatisticsSerializer extends Serializer<BinaryColumnStatistics>{

  @Override
  public void write(Kryo kryo, Output output, BinaryColumnStatistics object) {
    output.writeByte((byte)StatisticsKind.BINARY.ordinal());

    BinaryStatistics bs  = object.getBinaryStatistics();
    long numValues = object.getNumberOfValues();
    HiveBloomFilter filter = object.getBloomFilter();
    output.writeLong(numValues);
    output.writeLong(bs.getSum());
    kryo.writeObjectOrNull(output, filter, HiveBloomFilter.class);
  }

  @Override
  public BinaryColumnStatistics read(Kryo kryo, Input input,
      Class<? extends BinaryColumnStatistics> type) {
    
    long numValues = input.readLong();
    long sum = input.readLong();
    HiveBloomFilter filter = kryo.readObjectOrNull(input, HiveBloomFilter.class);
    BinaryStatistics bs = new BinaryStatistics(sum);
    return new BinaryColumnStatistics(numValues, filter, bs);
  }

}
