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

import java.lang.reflect.Constructor;
import java.lang.reflect.InvocationTargetException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Random;
import java.util.Set;

import org.apache.parquet.ShouldNeverHappenException;
import org.apache.parquet.bytes.BytesUtils;
import org.apache.parquet.column.Encoding;
import org.apache.parquet.column.EncodingStats;
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
import org.apache.parquet.schema.Type;
import org.apache.parquet.schema.LogicalTypeAnnotation;
import org.apache.parquet.schema.LogicalTypeAnnotation.IntervalLogicalTypeAnnotation;
import org.apache.parquet.schema.LogicalTypeAnnotation.MapKeyValueTypeAnnotation;
import org.apache.parquet.schema.LogicalTypeAnnotation.TimeUnit;
import org.apache.parquet.schema.MessageType;
import org.apache.parquet.schema.PrimitiveType;
import org.apache.parquet.schema.PrimitiveType.PrimitiveTypeName;
import org.apache.parquet.schema.Type.ID;
import org.apache.parquet.schema.Type.Repetition;

public class TestUtils {
  static PrimitiveTypeName[] statTypes = new PrimitiveTypeName[] {
      PrimitiveTypeName.BOOLEAN,
      PrimitiveTypeName.BINARY,
      PrimitiveTypeName.INT32,
      PrimitiveTypeName.INT64,
      PrimitiveTypeName.INT96,
      PrimitiveTypeName.FIXED_LEN_BYTE_ARRAY,
      PrimitiveTypeName.FLOAT,
      PrimitiveTypeName.DOUBLE
    };
  
  static Constructor<PrimitiveType> ptCstr;
  static Constructor<GroupType> gtCstr;
  static {
    try {
      ptCstr = PrimitiveType.class.getDeclaredConstructor(
        Repetition.class, 
        PrimitiveTypeName.class,
        int.class, 
        String.class, 
        LogicalTypeAnnotation.class, 
        ID.class, 
        ColumnOrder.class);
      ptCstr.setAccessible(true);
      gtCstr = GroupType.class.getDeclaredConstructor(
        Repetition.class,
        String.class,
        LogicalTypeAnnotation.class,
        List.class,
        ID.class);
      gtCstr.setAccessible(true);
    } catch (NoSuchMethodException | SecurityException e) {
      // TODO Auto-generated catch block
      e.printStackTrace();
    }
  }
  
  public static PrimitiveType createPrimitiveType(Repetition rep, PrimitiveTypeName typeName, int length, 
      String name, LogicalTypeAnnotation annotation, ID id, ColumnOrder order) 
  {
    try {
      return ptCstr.newInstance(rep, typeName, length, name, annotation, id, order);
    } catch (InstantiationException | IllegalAccessException | IllegalArgumentException
        | InvocationTargetException e) {
      // TODO Auto-generated catch block
      e.getCause().printStackTrace();
      e.printStackTrace();
      System.out.printf("Column Order=%s LogicalTypeAnnotation=%s\n", order, annotation);
    }
    return null;
  }
  
  public static PrimitiveType createRandomPrimitiveType(PrimitiveTypeName ptn) {
    Repetition rep = randomRepetition();
    int length = new Random().nextInt(1000);
    String name = "name";
    LogicalTypeAnnotation lta = null;// LogicalTypeAnnotation.listType();//randomLogicalTypeAnnotation();
    ID id = randomID();
    ColumnOrder order = randomColumnOrder();
    if (ptn == PrimitiveTypeName.INT96 || lta instanceof IntervalLogicalTypeAnnotation) {
      order = ColumnOrder.undefined();
    }
    return createPrimitiveType(rep, ptn, length, name, lta, id, order);
  }
  
  public static List<Type> randomListOfTypes(int size){
    List<Type> list = new ArrayList<>(size); 
    for (int i = 0; i < size; i++) {
      list.add(createRandomPrimitiveType());
    }
    return list;
  }
  
  public static PrimitiveType createRandomPrimitiveType() {
    Repetition rep = randomRepetition();
    PrimitiveTypeName ptn = randomPrimitiveTypeName();
    int length = new Random().nextInt(1000);
    String name = "name";
    LogicalTypeAnnotation lta = randomLogicalTypeAnnotation();
    ID id = randomID();
    ColumnOrder order = randomColumnOrder();
    if (ptn == PrimitiveTypeName.INT96 || lta instanceof IntervalLogicalTypeAnnotation) {
      order = ColumnOrder.undefined();
    }
    return createPrimitiveType(rep, ptn, length, name, lta, id, order);
  }
  
  public static PrimitiveType createSafeRandomPrimitiveType() {
    Repetition rep = randomRepetition();
    PrimitiveTypeName ptn = randomPrimitiveTypeName();
    int length = new Random().nextInt(1000);
    String name = "name";
    LogicalTypeAnnotation lta = null;
    ID id = randomID();
    ColumnOrder order = randomColumnOrder();
    if (ptn == PrimitiveTypeName.INT96 || lta instanceof IntervalLogicalTypeAnnotation) {
      order = ColumnOrder.undefined();
    }
    return createPrimitiveType(rep, ptn, length, name, lta, id, order);
  }
  
  public static Repetition randomRepetition() {
    Random r = new Random();
    int num = Repetition.values().length;
    return Repetition.values()[r.nextInt(num)];
  }
  
  public static PrimitiveTypeName randomPrimitiveTypeName() {
    Random r = new Random();
    int num = PrimitiveTypeName.values().length;
    return PrimitiveTypeName.values()[r.nextInt(num)];
  }
  
  public static ID randomID() {
    Random r = new Random();
    return new ID(r.nextInt(1000));
  }
  
  public static ColumnOrder randomColumnOrder() {
    Random r = new Random();
    int n = r.nextInt(2);
    
    switch(n) {
      case 0: return ColumnOrder.undefined();
      default: return ColumnOrder.typeDefined();
    }
  }
  
  public static Encoding randomEncoding() {
    int size = Encoding.values().length;
    Random r = new Random();
    return Encoding.values()[r.nextInt(size)];
  }
  
  public static CompressionCodecName randomCompressionCodecName() {
    int size = CompressionCodecName.values().length;
    Random r = new Random();
    return CompressionCodecName.values()[r.nextInt(size)];
  }
  
  public static LogicalTypeAnnotation randomLogicalTypeAnnotation() {
    Random r = new Random();
    int n = r.nextInt(14);
    
    switch(n) {
      case 0: return LogicalTypeAnnotation.bsonType();
      case 1: return LogicalTypeAnnotation.dateType();
      case 2: return LogicalTypeAnnotation.decimalType(38,  10);
      case 3: return LogicalTypeAnnotation.enumType();
      case 4: return IntervalLogicalTypeAnnotation.getInstance();
      case 5: return LogicalTypeAnnotation.intType(64, false);
      case 6: return LogicalTypeAnnotation.jsonType();
      case 7: return LogicalTypeAnnotation.listType();
      case 8: return MapKeyValueTypeAnnotation.getInstance();
      case 9: return LogicalTypeAnnotation.mapType();
      case 10: return LogicalTypeAnnotation.stringType();
      case 11: return LogicalTypeAnnotation.timeType(true, TimeUnit.MILLIS);
      case 12: return LogicalTypeAnnotation.timestampType(false, TimeUnit.MICROS);
      default: return LogicalTypeAnnotation.uuidType();
    }
  }
  
  @SuppressWarnings("deprecation")
  public static EncodingStats createEncodingStats() {
    EncodingStats.Builder builder = new EncodingStats.Builder();
    
    EncodingStats stats =  builder.withV2Pages()
     .addDataEncoding(Encoding.BIT_PACKED, 3)
     .addDataEncoding(Encoding.BYTE_STREAM_SPLIT, 4)
     .addDataEncoding(Encoding.DELTA_BINARY_PACKED, 5)
     .addDataEncoding(Encoding.DELTA_BYTE_ARRAY, 6)
     .addDataEncoding(Encoding.DELTA_LENGTH_BYTE_ARRAY, 7)
     .addDataEncoding(Encoding.PLAIN, 8)
     .addDataEncoding(Encoding.PLAIN_DICTIONARY, 9)
     .addDataEncoding(Encoding.RLE, 10)
     .addDataEncoding(Encoding.RLE_DICTIONARY, 11)
     .addDictEncoding(Encoding.BIT_PACKED, 3)
     .addDictEncoding(Encoding.BYTE_STREAM_SPLIT, 4)
     .addDictEncoding(Encoding.DELTA_BINARY_PACKED, 5)
     .addDictEncoding(Encoding.DELTA_BYTE_ARRAY, 6)
     .addDictEncoding(Encoding.DELTA_LENGTH_BYTE_ARRAY, 7)
     .addDictEncoding(Encoding.PLAIN, 8)
     .addDictEncoding(Encoding.PLAIN_DICTIONARY, 9)
     .addDictEncoding(Encoding.RLE, 10)
     .addDictEncoding(Encoding.RLE_DICTIONARY, 11).build();
    return stats;
  }
  
  @SuppressWarnings("deprecation")
  public static Statistics<?> createStatistics(PrimitiveType type){
    Statistics<?> stst = Statistics.createStats(type);
    stst.setNumNulls(1);
    byte[] minBytes = null;
    byte[] maxBytes = null;    
    Random r = new Random();
    if (stst instanceof IntStatistics) {
      int min = 1000 + r.nextInt(1000);
      int max = 2000 + r.nextInt(1000);
      minBytes = BytesUtils.intToBytes(min);
      maxBytes = BytesUtils.intToBytes(max);
    } else if (stst instanceof LongStatistics) {
      long min = 1000000 + r.nextInt(1000000);
      long max = 2000000 + r.nextInt(1000000);
      minBytes = BytesUtils.longToBytes(min);
      maxBytes = BytesUtils.longToBytes(max);
    } else if (stst instanceof FloatStatistics) {
      float min = r.nextFloat();
      float max = 1.0f + r.nextFloat();
      int imin = Float.floatToIntBits(min);
      int imax = Float.floatToIntBits(max);
      minBytes = BytesUtils.intToBytes(imin);
      maxBytes = BytesUtils.intToBytes(imax);
    } else if (stst instanceof DoubleStatistics) {
      double min = r.nextDouble();
      double max = 1.0f + r.nextDouble();
      long imin = Double.doubleToLongBits(min);
      long imax = Double.doubleToLongBits(max);
      minBytes = BytesUtils.longToBytes(imin);
      maxBytes = BytesUtils.longToBytes(imax);
    } else if (stst instanceof BooleanStatistics) {
      minBytes = new byte[] {(byte) 0};
      maxBytes = new byte[] {(byte) 1};
    } else {
      long min = 10000000 + r.nextInt(10000000);
      long max = 20000000 + r.nextInt(10000000);
      minBytes = BytesUtils.longToBytes(min);
      maxBytes = BytesUtils.longToBytes(max);
    }
    stst.setMinMaxFromBytes(minBytes, maxBytes);
    return stst;
  }
  
  
  
  public static ColumnPath getColumnPath(int level) {
    if (level > 'Z' - 'A' - 1) {
      level = 'Z' - 'A' - 1;
    }
    String[] arr = new String[level];
    for (int i = 0; i < level; i++) {
      arr[i] = Character.toString((char)('A' + (char)i));
    }
    return ColumnPath.get(arr);
  }
  
  public static GroupType createRandomGroupType(int listSize) {
    Repetition rep = randomRepetition();
    String name = "name";
    LogicalTypeAnnotation annot = randomLogicalTypeAnnotation();
    List<Type> list = randomListOfTypes(listSize);
    ID id = randomID();
    GroupType groupType = null;
    try {
      groupType = gtCstr.newInstance(rep, name, annot, list, id);
      return groupType;
    } catch (InstantiationException | IllegalAccessException | IllegalArgumentException
        | InvocationTargetException e) {
      // TODO Auto-generated catch block
      e.printStackTrace();
    }
    return null;
  }
  
  public static GroupType createRandomGroupTypeWithSubGroup(int listSize) {
    Repetition rep = randomRepetition();
    String name = "name";
    LogicalTypeAnnotation annot = randomLogicalTypeAnnotation();
    List<Type> list = randomListOfTypes(listSize - 1);
    GroupType gt = createRandomGroupType(listSize);
    list.add(gt);
    
    ID id = randomID();
    GroupType groupType = null;
    try {
      groupType = gtCstr.newInstance(rep, name, annot, list, id);
      return groupType;
    } catch (InstantiationException | IllegalAccessException | IllegalArgumentException
        | InvocationTargetException e) {
      // TODO Auto-generated catch block
      e.printStackTrace();
    }
    return null;
  }
  
  public static ColumnChunkMetaData createRandomColumnChunkMetaData() {
    Random r = new Random();
    ColumnPath path = getColumnPath(15);
    PrimitiveType ptype = createRandomPrimitiveType();
    CompressionCodecName codec = randomCompressionCodecName();
    EncodingStats encodingStats = createEncodingStats();
    int size = r.nextInt(50) + 1;
    Set<Encoding> encodings = new HashSet<Encoding>(size);
    for (int i = 0; i < size; i++) {
      encodings.add(randomEncoding());
    }
    Statistics<?> statistics = null;
    try {
      statistics = createStatistics(ptype);
    } catch(ShouldNeverHappenException | UnsupportedOperationException e){
      // Some LogicalTypeAnnotations conflict with PrimitiveType w.r.t to Statistics
      ptype = createSafeRandomPrimitiveType();
      statistics = createStatistics(ptype);
    }
    // There are two sub-classes of interest: IntColumnChunkMetaData and LongColumnChunkMetaData
    // they treat differently below numbers. For Long class we write them as is, for Int class
    // we need translate them before writing
    //  value = value + Integer.MIN_VALUE
    long firstDataPage = r.nextInt(100000000);
    long dictionaryPageOffset = r.nextInt(1000000000);
    long valueCount = r.nextInt(100000000);
    long totalSize = (long) r.nextInt(Integer.MAX_VALUE)  + Integer.MAX_VALUE / 2;
    long totalUncompressedSize = 2 * totalSize;
    return ColumnChunkMetaData.get(path, ptype, codec, encodingStats, encodings, statistics, 
      firstDataPage, dictionaryPageOffset, valueCount, totalSize, totalUncompressedSize);
  }
  
  public static boolean equals(GroupType gt1, GroupType gt2) {
    return gt1.getRepetition().equals(gt2.getRepetition()) &&
          gt1.getName().equals(gt2.getName()) &&
          gt1.getLogicalTypeAnnotation().equals(gt2.getLogicalTypeAnnotation()) &&
          gt1.getFields().equals(gt2.getFields());
  }
  
  public static boolean equals(MessageType gt1, MessageType gt2) {
    return gt1.getRepetition().equals(gt2.getRepetition()) &&
          gt1.getName().equals(gt2.getName()) &&
          gt1.getFields().equals(gt2.getFields());
  }
  
  public static boolean equals(EncodingStats one, EncodingStats two) {
    if(one.usesV2Pages() != two.usesV2Pages()) {
      return false;
    }
    Set<Encoding> set1 = one.getDataEncodings();
    Set<Encoding> set2 = two.getDataEncodings();
    if (set1.size() != set2.size()) {
      return false;
    }
    
    Iterator<Encoding> it1 = set1.iterator();
    Iterator<Encoding> it2 = set2.iterator();
    while(it1.hasNext()) {
      Encoding e1 = it1.next();
      Encoding e2 = it2.next();
      if (!e1.equals(e2)) {
        return false;
      }
      Number n1 = one.getNumDataPagesEncodedAs(e1);
      Number n2 = two.getNumDataPagesEncodedAs(e2);
      if (!n1.equals(n2)) {
        return false;
      }
    }
    
    set1 = one.getDictionaryEncodings();
    set2 = two.getDictionaryEncodings();
    if (set1.size() != set2.size()) {
      return false;
    }
    
    it1 = set1.iterator();
    it2 = set2.iterator();
    while(it1.hasNext()) {
      Encoding e1 = it1.next();
      Encoding e2 = it2.next();
      if (!e1.equals(e2)) {
        return false;
      }
      Number n1 = one.getNumDictionaryPagesEncodedAs(e1);
      Number n2 = two.getNumDictionaryPagesEncodedAs(e2);
      if (!n1.equals(n2)) {
        return false;
      }
    }
    return true;
  }
  
  public static boolean equals(ColumnChunkMetaData m1, ColumnChunkMetaData m2) {
    Statistics<?> stat1 = m1.getStatistics();
    Statistics<?> stat2 = m2.getStatistics();
    boolean res1 = stat1.equals(stat2);
    ColumnPath cp1 = m1.getPath();
    ColumnPath cp2 = m2.getPath();
    boolean res2 = cp1.equals(cp2);
    PrimitiveType pt1 = m1.getPrimitiveType();
    PrimitiveType pt2 = m2.getPrimitiveType();
    boolean res3 = pt1.equals(pt2);
    CompressionCodecName c1 = m1.getCodec();
    CompressionCodecName c2 = m2.getCodec();
    boolean res4 = c1.equals(c2);
    EncodingStats es1 = m1.getEncodingStats();
    EncodingStats es2 = m2.getEncodingStats();
    boolean res5 = equals(es1, es2);
    Set<Encoding> set1 = m1.getEncodings();
    Set<Encoding> set2 = m2.getEncodings();
    boolean res6 = set1.size() == set2.size();
    Iterator<Encoding> it1 = set1.iterator();
    boolean res7 = true;
    while (it1.hasNext()) {
      Encoding e1 = it1.next();
      res7 = res7 && set2.contains(e1);
    }
    long firstDataPage1 = m1.getFirstDataPageOffset();
    long firstDataPage2 = m2.getFirstDataPageOffset();
    long dictPageOffset1 = m1.getDictionaryPageOffset();
    long dictPageOffset2 = m2.getDictionaryPageOffset();
    long valueCount1 = m1.getValueCount();
    long valueCount2 = m2.getValueCount();
    long totalSize1 = m1.getTotalSize();
    long totalSize2 = m2.getTotalSize();
    
    long totalUncompressedSize1 = m1.getTotalUncompressedSize();
    long totalUncompressedSize2 = m2.getTotalUncompressedSize();
    
    boolean res8 = firstDataPage1 == firstDataPage2 && dictPageOffset1 == dictPageOffset2 &&
          valueCount1 == valueCount2 && totalSize1 == totalSize2 &&
          totalUncompressedSize1 == totalUncompressedSize2;
    return res1 && res2 && res3 && res4 && res5 && res6 && res7 && res8;
  }
  
  public static List<ColumnChunkMetaData> createListOfColumnChunkMetaData(int size){
    ArrayList<ColumnChunkMetaData> list = new ArrayList<>();
    for (int i = 0; i < size; i++) {
      list.add(createRandomColumnChunkMetaData());
    }
    return list;
  }
  
  public static BlockMetaData createRandomBlockMetaData(int numColumns, int ordinal) {
    return createRandomBlockMetaData(numColumns, ordinal, "/some-path");
  }
  
  public static BlockMetaData createRandomBlockMetaData(int numColumns, int ordinal, String path) {
    BlockMetaData block = new BlockMetaData();
    List<ColumnChunkMetaData> list = createListOfColumnChunkMetaData(numColumns);
    list.stream().forEach(x -> block.addColumn(x));
    block.setOrdinal(ordinal);
    block.setPath(path);
    block.setRowCount(560000);
    block.setTotalByteSize(10000000);
    return block;
  }
  
  public static boolean equals(BlockMetaData block1, BlockMetaData block2) {
    boolean res1 = block1.getOrdinal() == block2.getOrdinal();
    boolean res2 = (block1.getPath() == null && block2.getPath() == null) ||
        block1.getPath().equals(block2.getPath());
    boolean res3 = block1.getRowCount() == block2.getRowCount();
    boolean res4 = block1.getTotalByteSize() == block2.getTotalByteSize();
    return res1 && res2 && res3 && res4;
  }
  
  public static MessageType createRandomMessageType() {
    String name = "name";
    List<Type> list = TestUtils.randomListOfTypes(100);
    MessageType mt = new MessageType(name, list);
    return mt;
  }
  
  public static FileMetaData createRandomFileMetaData() {
    MessageType mt = createRandomMessageType();
    Random r = new Random();
    String createdBy = "create by user:" + r.nextInt(10000);
    
    Map<String, String> meta = new HashMap<String, String>(); 
    int size = r.nextInt(100) + 10;
    for (int i= 0; i < size; i++) {
      meta.put("key" + i, "meta"+i);
    }
    return new FileMetaData(mt, meta, createdBy);
  }
  
  public static FileMetaData createRandomFileMetaDataWithNull() {
    MessageType mt = createRandomMessageType();
    Random r = new Random();
    String createdBy = null;
    
    Map<String, String> meta = new HashMap<String, String>(); 
    int size = r.nextInt(100) + 10;
    for (int i= 0; i < size; i++) {
      meta.put("key" + i, "meta"+i);
    }
    return new FileMetaData(mt, meta, createdBy);
  }
  
  public static ParquetMetadata createRandomParquetMetadata() {
    Random r = new Random();
    int size = r.nextInt(100) + 10;
    List<BlockMetaData> blocks = new ArrayList<>();
    for (int i = 0; i < size; i++) {
      blocks.add(createRandomBlockMetaData(size, i));
    }
    FileMetaData fileMeta = createRandomFileMetaData();
    return new ParquetMetadata(fileMeta, blocks);
  }
  
  public static boolean equals(FileMetaData m1, FileMetaData m2) {
    boolean res1 = equals(m1.getSchema(), m2.getSchema());
    boolean res2 = (m1.getCreatedBy() == null && m2.getCreatedBy() == null) ||
        m1.getCreatedBy().equals(m2.getCreatedBy());
    Map<String, String> map1 = m1.getKeyValueMetaData();
    Map<String, String> map2 = m2.getKeyValueMetaData();
    boolean res3 = equals(map1, map2);
    return res1 && res2 && res3;
  }
  
  public static <K,V> boolean equals(Map<K,V> one, Map<K,V> two) {
    Set<K> set1 = one.keySet();
    Set<K> set2 = two.keySet();
    boolean res = equals(set1, set2);
    if (!res) {
      return false;
    }
    
    Iterator<K> it = set1.iterator();
    while(it.hasNext()) {
      K key = it.next();
      V v1 = one.get(key);
      V v2 = two.get(key);
      if (v1 == null) {
        return false;
      }
      if (!v1.equals(v2)) {
        return false;
      }
    }
    return true;
  }
  
  public static <K> boolean equals(Set<K> one, Set<K> two) {
    if (one == null && two != null) {
      return false;
    } else if (one != null && two == null) {
      return false;
    } else if (one == null && two == null) {
      return true;
    }
    if (one.size() != two.size()) {
      return false;
    }
    return one.containsAll(two);
  }
  
  public static boolean equals(ParquetMetadata m1, ParquetMetadata m2) {
    boolean res1 = equals(m1.getFileMetaData(), m2.getFileMetaData());
    if (!res1) {
      return false;
    }
    List<BlockMetaData> blocks1 = m1.getBlocks();
    List<BlockMetaData> blocks2 = m2.getBlocks();
    boolean res2 = blocks1.size() == blocks2.size();
    if (!res2) {
      return false;
    }
    for (int i = 0; i < blocks1.size(); i++) {
      BlockMetaData block1 = blocks1.get(i);
      BlockMetaData block2 = blocks2.get(i);
      if (!equals(block1, block2)) {
        return false;
      }
    }
    return true;
  }
}
