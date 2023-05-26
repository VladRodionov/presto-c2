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

import static org.testng.Assert.assertTrue;

import java.util.Arrays;
import java.util.Random;

import org.testng.annotations.Test;

import com.esotericsoftware.kryo.kryo5.io.Input;
import com.esotericsoftware.kryo.kryo5.io.Output;

import io.airlift.slice.Slice;
import io.airlift.slice.Slices;

public class TestSliceSerializer extends TestSerializerBase {

  @Test
  public void testSliceSerializer() {
    Output out = new Output(1 << 16);
    Random r = new Random();
    int size  = 1024;
    byte[] buffer = new byte[size];
    r.nextBytes(buffer);
    // 1 direct slice
    Slice slice = Slices.allocateDirect(size);
    slice.setBytes(0, buffer);
    kryo.writeObject(out, slice);
    Input in = new Input(out.getBuffer());
    Slice read = kryo.readObject(in, Slice.class);
    byte[] buf = new byte[size];
    read.getBytes(0, buf);
    assertTrue(Arrays.equals(buffer, buf));
    // 2 byte buffer
    slice = Slices.wrappedBuffer(buffer);
    kryo.writeObject(out, slice);
    in = new Input(out.getBuffer());
    read = kryo.readObject(in, Slice.class);
    buf = new byte[size];
    read.getBytes(0, buf);
    assertTrue(Arrays.equals(buffer, buf));
    // 3 short buffer
    slice = Slices.wrappedShortArray(new short[size / 2]);
    slice.setBytes(0, buffer);
    kryo.writeObject(out, slice);
    in = new Input(out.getBuffer());
    read = kryo.readObject(in, Slice.class);
    buf = new byte[size];
    read.getBytes(0, buf);
    assertTrue(Arrays.equals(buffer, buf));
    // 4 boolean buffer
    slice = Slices.wrappedBooleanArray(new boolean[size]);
    slice.setBytes(0, buffer);
    kryo.writeObject(out, slice);
    in = new Input(out.getBuffer());
    read = kryo.readObject(in, Slice.class);
    buf = new byte[size];
    read.getBytes(0, buf);
    assertTrue(Arrays.equals(buffer, buf));
    // 5 int buffer
    slice = Slices.wrappedIntArray(new int[size / 4]);
    slice.setBytes(0, buffer);
    kryo.writeObject(out, slice);
    in = new Input(out.getBuffer());
    read = kryo.readObject(in, Slice.class);
    buf = new byte[size];
    read.getBytes(0, buf);
    assertTrue(Arrays.equals(buffer, buf));
    // 6 long buffer
    slice = Slices.wrappedLongArray(new long[size / 8]);
    slice.setBytes(0, buffer);
    kryo.writeObject(out, slice);
    in = new Input(out.getBuffer());
    read = kryo.readObject(in, Slice.class);
    buf = new byte[size];
    read.getBytes(0, buf);
    assertTrue(Arrays.equals(buffer, buf));
    // 7 float buffer
    slice = Slices.wrappedFloatArray(new float[size / 4]);
    slice.setBytes(0, buffer);
    kryo.writeObject(out, slice);
    in = new Input(out.getBuffer());
    read = kryo.readObject(in, Slice.class);
    buf = new byte[size];
    read.getBytes(0, buf);
    assertTrue(Arrays.equals(buffer, buf));
    // 8 float buffer
    slice = Slices.wrappedDoubleArray(new double[size / 8]);
    slice.setBytes(0, buffer);
    kryo.writeObject(out, slice);
    in = new Input(out.getBuffer());
    read = kryo.readObject(in, Slice.class);
    buf = new byte[size];
    read.getBytes(0, buf);
    assertTrue(Arrays.equals(buffer, buf));
  }  
 
}
