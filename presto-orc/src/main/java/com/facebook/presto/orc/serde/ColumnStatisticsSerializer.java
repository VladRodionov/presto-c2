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
import com.facebook.presto.orc.metadata.statistics.BooleanColumnStatistics;
import com.facebook.presto.orc.metadata.statistics.ColumnStatistics;
import com.facebook.presto.orc.metadata.statistics.DateColumnStatistics;
import com.facebook.presto.orc.metadata.statistics.DecimalColumnStatistics;
import com.facebook.presto.orc.metadata.statistics.DoubleColumnStatistics;
import com.facebook.presto.orc.metadata.statistics.IntegerColumnStatistics;
import com.facebook.presto.orc.metadata.statistics.MapColumnStatistics;
import com.facebook.presto.orc.metadata.statistics.StringColumnStatistics;

public class ColumnStatisticsSerializer extends Serializer<ColumnStatistics>{

  @Override
  public void write(Kryo kryo, Output output, ColumnStatistics object) {
    // not used
  }

  @SuppressWarnings({ "unchecked", "rawtypes" })
  @Override
  public ColumnStatistics read(Kryo kryo, Input input, Class<? extends ColumnStatistics> type) {
    StatisticsKind kind = StatisticsKind.values()[input.readByte()];
    Class cls = null;
    switch (kind) {
      case BINARY:
        cls = BinaryColumnStatistics.class;
        break;
      case BOOLEAN:
        cls = BooleanColumnStatistics.class;
        break;
      case DATE:
        cls = DateColumnStatistics.class;
        break;
      case DECIMAL:
        cls = DecimalColumnStatistics.class;
        break;
      case DOUBLE:
        cls = DoubleColumnStatistics.class;
        break;
      case INTEGER:
        cls = IntegerColumnStatistics.class;
        break;
      case MAP:
        cls = MapColumnStatistics.class;
        break;
      case STRING:
        cls = StringColumnStatistics.class;
        break;
    }
    return (ColumnStatistics) kryo.readObject(input, cls);
  }

}
