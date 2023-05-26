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

import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertTrue;

import java.util.Random;

import org.testng.annotations.Test;

import com.esotericsoftware.kryo.kryo5.io.Input;
import com.esotericsoftware.kryo.kryo5.io.Output;
import com.facebook.presto.orc.metadata.DwrfStripeCacheData;
import com.facebook.presto.orc.metadata.DwrfStripeCacheMode;

import io.airlift.slice.Slice;
import io.airlift.slice.Slices;

public class TestDwrfStripeCacheDataSerializer extends TestSerializerBase{

  @Test
  public void testDwrfStripeCacheDataSerializer() {
    byte[] buf = new byte[100];
    Random r = new Random();
    r.nextBytes(buf);
    Slice slice = Slices.wrappedBuffer(buf, 5, 50);
    DwrfStripeCacheData data = new DwrfStripeCacheData(slice, 100, DwrfStripeCacheMode.INDEX_AND_FOOTER);
    Output out = new Output(1<<16);
    kryo.writeObject(out, data);
    Input in = new Input(out.getBuffer());
    DwrfStripeCacheData read = kryo.readObject(in, DwrfStripeCacheData.class);
    
    assertTrue(slice.compareTo(read.getDwrfStripeCacheSlice()) == 0);
    assertEquals(100, read.getDwrfStripeCacheSize());
    assertEquals(DwrfStripeCacheMode.INDEX_AND_FOOTER, read.getDwrfStripeCacheMode());
    assertEquals(5, read.getOriginalOffset());
    
  }
}
