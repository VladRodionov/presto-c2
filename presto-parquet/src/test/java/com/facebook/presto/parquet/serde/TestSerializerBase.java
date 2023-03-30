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
package com.facebook.presto.parquet.serde;

import java.util.ArrayList;
import java.util.HashMap;

import org.apache.parquet.column.Encoding;
import org.apache.parquet.column.EncodingStats;
import org.apache.parquet.column.statistics.BinaryStatistics;
import org.apache.parquet.column.statistics.BooleanStatistics;
import org.apache.parquet.column.statistics.DoubleStatistics;
import org.apache.parquet.column.statistics.FloatStatistics;
import org.apache.parquet.column.statistics.IntStatistics;
import org.apache.parquet.column.statistics.LongStatistics;
import org.apache.parquet.column.statistics.Statistics;
import org.apache.parquet.hadoop.metadata.BlockMetaData;
import org.apache.parquet.hadoop.metadata.ColumnChunkMetaData;
import org.apache.parquet.hadoop.metadata.ColumnPath;
import org.apache.parquet.hadoop.metadata.CompressionCodecName;
import org.apache.parquet.hadoop.metadata.FileMetaData;
import org.apache.parquet.hadoop.metadata.ParquetMetadata;
import org.apache.parquet.schema.ColumnOrder;
import org.apache.parquet.schema.GroupType;
import org.apache.parquet.schema.LogicalTypeAnnotation;
import org.apache.parquet.schema.MessageType;
import org.apache.parquet.schema.PrimitiveType;
import org.apache.parquet.schema.LogicalTypeAnnotation.BsonLogicalTypeAnnotation;
import org.apache.parquet.schema.LogicalTypeAnnotation.DateLogicalTypeAnnotation;
import org.apache.parquet.schema.LogicalTypeAnnotation.DecimalLogicalTypeAnnotation;
import org.apache.parquet.schema.LogicalTypeAnnotation.EnumLogicalTypeAnnotation;
import org.apache.parquet.schema.LogicalTypeAnnotation.IntLogicalTypeAnnotation;
import org.apache.parquet.schema.LogicalTypeAnnotation.IntervalLogicalTypeAnnotation;
import org.apache.parquet.schema.LogicalTypeAnnotation.JsonLogicalTypeAnnotation;
import org.apache.parquet.schema.LogicalTypeAnnotation.ListLogicalTypeAnnotation;
import org.apache.parquet.schema.LogicalTypeAnnotation.MapKeyValueTypeAnnotation;
import org.apache.parquet.schema.LogicalTypeAnnotation.MapLogicalTypeAnnotation;
import org.apache.parquet.schema.LogicalTypeAnnotation.StringLogicalTypeAnnotation;
import org.apache.parquet.schema.LogicalTypeAnnotation.TimeLogicalTypeAnnotation;
import org.apache.parquet.schema.LogicalTypeAnnotation.TimestampLogicalTypeAnnotation;
import org.apache.parquet.schema.LogicalTypeAnnotation.UUIDLogicalTypeAnnotation;
import org.apache.parquet.schema.PrimitiveType.PrimitiveTypeName;
import org.apache.parquet.schema.Type.ID;
import org.apache.parquet.schema.Type.Repetition;
import org.testng.annotations.BeforeClass;

import com.carrot.cache.ObjectCache;
import com.esotericsoftware.kryo.kryo5.Kryo;
import com.esotericsoftware.kryo.kryo5.Serializer;
import com.esotericsoftware.kryo.kryo5.SerializerFactory;
import com.facebook.presto.parquet.cache.ParquetFileMetadata;

public class TestSerializerBase {

  static Kryo kryo;
  
  @BeforeClass
  public static void setUp() {

    kryo = new Kryo();
//    @SuppressWarnings("rawtypes")
//    SerializerFactory factory = new SerializerFactory() {
//      @Override
//      public Serializer<?> newSerializer(Kryo kryo, Class type) {
//        return new ColumnChunkMetaDataSerializer();
//      }
//
//      @Override
//      public boolean isSupported(Class type) {
//        String className = type.getName();
//        if (className.equals("org.apache.parquet.hadoop.metadata.IntColumnChunkMetaData")
//            || className.equals("org.apache.parquet.hadoop.metadata.LongColumnChunkMetaData")) {
//          return true;
//        } else {
//          return false;
//        }
//      }
//    };
//    kryo.setRegistrationRequired(false);
    //kryo.setDefaultSerializer(factory);
    try {
      Class<?> intMetaDataClass =
          Class.forName("org.apache.parquet.hadoop.metadata.IntColumnChunkMetaData");
      Class<?> longMetaDataClass =
          Class.forName("org.apache.parquet.hadoop.metadata.LongColumnChunkMetaData");
      kryo.register(intMetaDataClass, new ColumnChunkMetaDataSerializer());
      kryo.register(longMetaDataClass, new ColumnChunkMetaDataSerializer());

    } catch (ClassNotFoundException e) {
      throw new RuntimeException(e);
    }

    // Level 0
    kryo.register(ParquetFileMetadata.class, new ParquetFileMetadataSerializer());
    // Level 1
    kryo.register(ParquetMetadata.class, new ParquetMetadataSerializer());
    // Level 2
    kryo.register(FileMetaData.class, new FileMetaDataSerializer());
    kryo.register(ArrayList.class);
    kryo.register(BlockMetaData.class, new BlockMetaDataSerializer());
    // Level 3 - FileMetaData
    kryo.register(HashMap.class);
    kryo.register(MessageType.class, new MessageTypeSerializer());
    // We skip InternalFileDecryptor - it is null according to the source code of MetadataReader
    // Level 3 BlockMetaData
    kryo.register(ColumnChunkMetaData.class, new ColumnChunkMetaDataSerializer());
    // Level 4: MessageType - GroupType
    kryo.register(GroupType.class, new GroupTypeSerializer());
    // Level 4: ColumnChunkMetaData -
    kryo.register(ColumnPath.class, new ColumnPathSerializer());
    kryo.register(PrimitiveType.class, new PrimitiveTypeSerializer());
    kryo.register(CompressionCodecName.class, new CompressionCodecNameSerializer());
    kryo.register(EncodingStats.class, new EncodingStatsSerializer());
    kryo.register(Encoding.class, new EncodingSerializer());
    kryo.register(Statistics.class, new StatisticsSerializer());
    kryo.register(IntStatistics.class, new StatisticsSerializer());
    kryo.register(LongStatistics.class, new StatisticsSerializer());
    kryo.register(FloatStatistics.class, new StatisticsSerializer());
    kryo.register(DoubleStatistics.class, new StatisticsSerializer());
    kryo.register(BooleanStatistics.class, new StatisticsSerializer());
    kryo.register(BinaryStatistics.class, new StatisticsSerializer());

    kryo.register(Repetition.class, new RepetitionSerializer());
    kryo.register(PrimitiveTypeName.class, new PrimitiveTypeNameSerializer());

    kryo.register(LogicalTypeAnnotation.class, new LogicalTypeAnnotationSerializer());
    kryo.register(BsonLogicalTypeAnnotation.class, new LogicalTypeAnnotationSerializer());
    kryo.register(DateLogicalTypeAnnotation.class, new LogicalTypeAnnotationSerializer());
    kryo.register(DecimalLogicalTypeAnnotation.class, new LogicalTypeAnnotationSerializer());
    kryo.register(EnumLogicalTypeAnnotation.class, new LogicalTypeAnnotationSerializer());
    kryo.register(IntervalLogicalTypeAnnotation.class, new LogicalTypeAnnotationSerializer());
    kryo.register(IntLogicalTypeAnnotation.class, new LogicalTypeAnnotationSerializer());
    kryo.register(JsonLogicalTypeAnnotation.class, new LogicalTypeAnnotationSerializer());
    kryo.register(ListLogicalTypeAnnotation.class, new LogicalTypeAnnotationSerializer());
    kryo.register(MapKeyValueTypeAnnotation.class, new LogicalTypeAnnotationSerializer());
    kryo.register(MapLogicalTypeAnnotation.class, new LogicalTypeAnnotationSerializer());
    kryo.register(StringLogicalTypeAnnotation.class, new LogicalTypeAnnotationSerializer());
    kryo.register(TimeLogicalTypeAnnotation.class, new LogicalTypeAnnotationSerializer());
    kryo.register(TimestampLogicalTypeAnnotation.class, new LogicalTypeAnnotationSerializer());
    kryo.register(UUIDLogicalTypeAnnotation.class, new LogicalTypeAnnotationSerializer());

    kryo.register(ID.class, new IDSerializer());
    kryo.register(ColumnOrder.class, new ColumnOrderSerializer());
  }
}
