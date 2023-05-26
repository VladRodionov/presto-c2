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

import io.airlift.slice.Slice;
import io.airlift.slice.Slices;
import static sun.misc.Unsafe.ARRAY_BOOLEAN_INDEX_SCALE;
import static sun.misc.Unsafe.ARRAY_DOUBLE_INDEX_SCALE;
import static sun.misc.Unsafe.ARRAY_FLOAT_INDEX_SCALE;
import static sun.misc.Unsafe.ARRAY_INT_INDEX_SCALE;
import static sun.misc.Unsafe.ARRAY_LONG_INDEX_SCALE;
import static sun.misc.Unsafe.ARRAY_SHORT_INDEX_SCALE;

public class SliceSerializer extends Serializer<Slice>{
  
  static enum SliceType {
    DIRECT,
    BYTE,
    BOOLEAN,
    SHORT,
    INT,
    LONG,
    FLOAT,
    DOUBLE;
  }
  
  public SliceSerializer() {
    
  }
  @Override
  public void write(Kryo kryo, Output output, Slice object) {
    Object base = object.getBase();
    SliceType type = typeOf(base);
    output.writeByte((byte) type.ordinal());
    byte[] data = object.getBytes();
    output.writeInt(data.length);
    output.writeBytes(data);
  }

  private SliceType typeOf(Object base) {
    SliceType type;
    if (base == null) {
      type = SliceType.DIRECT; // direct memory
    } else if (base instanceof byte[]) {
      type = SliceType.BYTE;
    } else if (base instanceof boolean[]) {
      type = SliceType.BOOLEAN;
    } else if (base instanceof short[]) {
      type = SliceType.SHORT;
    } else if (base instanceof int[]) {
      type = SliceType.INT;
    } else if (base instanceof long[]) {
      type = SliceType.LONG;
    } else if (base instanceof float[]) {
      type = SliceType.FLOAT;
    } else if (base instanceof double[]) {
      type = SliceType.DOUBLE;
    } else {
      throw new RuntimeException("Unsupported slice type for base of class " + base.getClass());
    }
    return type;
  }
  
  @Override
  public Slice read(Kryo kryo, Input input,
      Class<? extends Slice> type) {
    SliceType sliceType = SliceType.values()[input.readByte()];
    int size = input.readInt();
    byte[] buf = new byte[size];
    input.readBytes(buf);
    return sliceOf(sliceType, buf);
  }
  
  @SuppressWarnings("restriction")
  private Slice sliceOf(SliceType sliceType, byte[] buf) {
    Slice slice = null;
    switch (sliceType) {
      case DIRECT: 
        slice = Slices.allocateDirect(buf.length); 
        break;
      case BYTE: 
        slice = Slices.wrappedBuffer(buf);
        return slice;
      case BOOLEAN:
        boolean[] arr = new boolean[buf.length / ARRAY_BOOLEAN_INDEX_SCALE];
        slice = Slices.wrappedBooleanArray(arr);
        break;
      case SHORT:
        short[] shortArr = new short[buf.length / ARRAY_SHORT_INDEX_SCALE];
        slice = Slices.wrappedShortArray(shortArr);
        break;
      case INT:
        int[] intArr = new int[buf.length / ARRAY_INT_INDEX_SCALE];
        slice = Slices.wrappedIntArray(intArr);
        break;
      case LONG:
        long[] longArr = new long[buf.length / ARRAY_LONG_INDEX_SCALE];
        slice = Slices.wrappedLongArray(longArr);
        break;
      case FLOAT:
        float[] floatArr = new float[buf.length / ARRAY_FLOAT_INDEX_SCALE];
        slice = Slices.wrappedFloatArray(floatArr);
        break;
      case DOUBLE:
        double[] doubleArr = new double[buf.length / ARRAY_DOUBLE_INDEX_SCALE];
        slice = Slices.wrappedDoubleArray(doubleArr);
        break;
    } 
    slice.setBytes(0, buf);
    return slice;
  }
}
