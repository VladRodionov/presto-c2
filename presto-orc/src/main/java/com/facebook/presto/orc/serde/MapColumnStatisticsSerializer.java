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

import java.util.ArrayList;
import java.util.List;
import com.esotericsoftware.kryo.kryo5.Kryo;
import com.esotericsoftware.kryo.kryo5.Serializer;
import com.esotericsoftware.kryo.kryo5.io.Input;
import com.esotericsoftware.kryo.kryo5.io.Output;
import com.facebook.presto.orc.metadata.statistics.HiveBloomFilter;
import com.facebook.presto.orc.metadata.statistics.MapColumnStatistics;
import com.facebook.presto.orc.metadata.statistics.MapStatistics;
import com.facebook.presto.orc.metadata.statistics.MapStatisticsEntry;


public class MapColumnStatisticsSerializer extends Serializer<MapColumnStatistics>{

  @Override
  public void write(Kryo kryo, Output output, MapColumnStatistics object) {
    output.writeByte((byte)StatisticsKind.MAP.ordinal());

    long numValues = object.getNumberOfValues();
    HiveBloomFilter filter = object.getBloomFilter();
    MapStatistics ms = object.getMapStatistics();
    List<MapStatisticsEntry> entries = ms.getEntries();
    output.writeLong(numValues);
    kryo.writeObjectOrNull(output, filter, HiveBloomFilter.class);
    output.writeInt(entries.size());
    for (int i = 0; i < entries.size(); i++) {
      MapStatisticsEntry entry = entries.get(i);
      kryo.writeObject(output, entry);
    }
  }

  @Override
  public MapColumnStatistics read(Kryo kryo, Input input,
      Class<? extends MapColumnStatistics> type) {
    long numValues = input.readLong();
    HiveBloomFilter filter = kryo.readObjectOrNull(input, HiveBloomFilter.class);
    int size = input.readInt();
    List<MapStatisticsEntry> entries = new ArrayList<>();
    for (int i = 0; i < size; i++) {
      MapStatisticsEntry entry = kryo.readObject(input, MapStatisticsEntry.class);
      entries.add(entry);
    }
    MapStatistics ms = new MapStatistics(entries);
    return new MapColumnStatistics(numValues, filter, ms);
  }

}
