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

import static org.testng.Assert.assertEquals;

import org.apache.parquet.schema.LogicalTypeAnnotation;
import org.apache.parquet.schema.LogicalTypeAnnotation.IntervalLogicalTypeAnnotation;
import org.apache.parquet.schema.LogicalTypeAnnotation.MapKeyValueTypeAnnotation;
import org.apache.parquet.schema.LogicalTypeAnnotation.TimeUnit;
import org.testng.annotations.Test;

import com.esotericsoftware.kryo.kryo5.io.Input;
import com.esotericsoftware.kryo.kryo5.io.Output;

public class TestLogicalTypeAnnotationSerializer extends TestSerializerBase{

  @Test
  public void testLogicalTypeAnnotationSerializer() {
  

    Output out = new Output(1 << 16);

    LogicalTypeAnnotation lta = LogicalTypeAnnotation.bsonType();
    kryo.writeObject(out, lta);
    byte[] buf = out.getBuffer();
    Input in = new Input(buf);
    LogicalTypeAnnotation lta1 = kryo.readObject(in, LogicalTypeAnnotation.class);
    assertEquals(lta, lta1);
    out.reset();
    
    lta = LogicalTypeAnnotation.dateType();
    kryo.writeObject(out, lta);
    buf = out.getBuffer();
    in = new Input(buf);
    lta1 = kryo.readObject(in, LogicalTypeAnnotation.class);
    assertEquals(lta, lta1);
    out.reset();
    
    lta = LogicalTypeAnnotation.decimalType(38, 10);
    kryo.writeObject(out, lta);
    buf = out.getBuffer();
    in = new Input(buf);
    lta1 = kryo.readObject(in, LogicalTypeAnnotation.class);
    assertEquals(lta, lta1);
    out.reset();
    
    lta = LogicalTypeAnnotation.enumType();
    kryo.writeObject(out, lta);
    buf = out.getBuffer();
    in = new Input(buf);
    lta1 = kryo.readObject(in, LogicalTypeAnnotation.class);
    assertEquals(lta, lta1);
    out.reset();
    
    lta = IntervalLogicalTypeAnnotation.getInstance();
    kryo.writeObject(out, lta);
    buf = out.getBuffer();
    in = new Input(buf);
    lta1 = kryo.readObject(in, LogicalTypeAnnotation.class);
    assertEquals(lta, lta1);
    out.reset();
    
    lta = LogicalTypeAnnotation.intType(8, true);
    kryo.writeObject(out, lta);
    buf = out.getBuffer();
    in = new Input(buf);
    lta1 = kryo.readObject(in, LogicalTypeAnnotation.class);
    assertEquals(lta, lta1);
    out.reset();
    
    
    lta = LogicalTypeAnnotation.intType(8, false);
    kryo.writeObject(out, lta);
    buf = out.getBuffer();
    in = new Input(buf);
    lta1 = kryo.readObject(in, LogicalTypeAnnotation.class);
    assertEquals(lta, lta1);
    out.reset();
    
    lta = LogicalTypeAnnotation.intType(16, true);
    kryo.writeObject(out, lta);
    buf = out.getBuffer();
    in = new Input(buf);
    lta1 = kryo.readObject(in, LogicalTypeAnnotation.class);
    assertEquals(lta, lta1);
    out.reset();
    
    
    lta = LogicalTypeAnnotation.intType(16, false);
    kryo.writeObject(out, lta);
    buf = out.getBuffer();
    in = new Input(buf);
    lta1 = kryo.readObject(in, LogicalTypeAnnotation.class);
    assertEquals(lta, lta1);
    out.reset();
    
    lta = LogicalTypeAnnotation.intType(32, true);
    kryo.writeObject(out, lta);
    buf = out.getBuffer();
    in = new Input(buf);
    lta1 = kryo.readObject(in, LogicalTypeAnnotation.class);
    assertEquals(lta, lta1);
    out.reset();
    
    
    lta = LogicalTypeAnnotation.intType(32, false);
    kryo.writeObject(out, lta);
    buf = out.getBuffer();
    in = new Input(buf);
    lta1 = kryo.readObject(in, LogicalTypeAnnotation.class);
    assertEquals(lta, lta1);
    out.reset();
    
    lta = LogicalTypeAnnotation.intType(64, true);
    kryo.writeObject(out, lta);
    buf = out.getBuffer();
    in = new Input(buf);
    lta1 = kryo.readObject(in, LogicalTypeAnnotation.class);
    assertEquals(lta, lta1);
    out.reset();
    
    
    lta = LogicalTypeAnnotation.intType(64, false);
    kryo.writeObject(out, lta);
    buf = out.getBuffer();
    in = new Input(buf);
    lta1 = kryo.readObject(in, LogicalTypeAnnotation.class);
    assertEquals(lta, lta1);
    out.reset();
    
    lta = LogicalTypeAnnotation.jsonType();
    kryo.writeObject(out, lta);
    buf = out.getBuffer();
    in = new Input(buf);
    lta1 = kryo.readObject(in, LogicalTypeAnnotation.class);
    assertEquals(lta, lta1);
    out.reset();
    
    lta = LogicalTypeAnnotation.listType();
    kryo.writeObject(out, lta);
    buf = out.getBuffer();
    in = new Input(buf);
    lta1 = kryo.readObject(in, LogicalTypeAnnotation.class);
    assertEquals(lta, lta1);
    out.reset();
    
    lta = MapKeyValueTypeAnnotation.getInstance();
    kryo.writeObject(out, lta);
    buf = out.getBuffer();
    in = new Input(buf);
    lta1 = kryo.readObject(in, LogicalTypeAnnotation.class);
    assertEquals(lta, lta1);
    out.reset();
    
    lta = LogicalTypeAnnotation.mapType();
    kryo.writeObject(out, lta);
    buf = out.getBuffer();
    in = new Input(buf);
    lta1 = kryo.readObject(in, LogicalTypeAnnotation.class);
    assertEquals(lta, lta1);
    out.reset();
    
    lta = LogicalTypeAnnotation.stringType();
    kryo.writeObject(out, lta);
    buf = out.getBuffer();
    in = new Input(buf);
    lta1 = kryo.readObject(in, LogicalTypeAnnotation.class);
    assertEquals(lta, lta1);
    out.reset();
    
    lta = LogicalTypeAnnotation.timeType(true, TimeUnit.MICROS);
    kryo.writeObject(out, lta);
    buf = out.getBuffer();
    in = new Input(buf);
    lta1 = kryo.readObject(in, LogicalTypeAnnotation.class);
    assertEquals(lta, lta1);
    out.reset();
    
    lta = LogicalTypeAnnotation.timeType(false, TimeUnit.MICROS);
    kryo.writeObject(out, lta);
    buf = out.getBuffer();
    in = new Input(buf);
    lta1 = kryo.readObject(in, LogicalTypeAnnotation.class);
    assertEquals(lta, lta1);
    out.reset();
    
    lta = LogicalTypeAnnotation.timeType(true, TimeUnit.MILLIS);
    kryo.writeObject(out, lta);
    buf = out.getBuffer();
    in = new Input(buf);
    lta1 = kryo.readObject(in, LogicalTypeAnnotation.class);
    assertEquals(lta, lta1);
    out.reset();
    
    lta = LogicalTypeAnnotation.timeType(false, TimeUnit.MILLIS);
    kryo.writeObject(out, lta);
    buf = out.getBuffer();
    in = new Input(buf);
    lta1 = kryo.readObject(in, LogicalTypeAnnotation.class);
    assertEquals(lta, lta1);
    out.reset();
    
    lta = LogicalTypeAnnotation.timeType(true, TimeUnit.NANOS);
    kryo.writeObject(out, lta);
    buf = out.getBuffer();
    in = new Input(buf);
    lta1 = kryo.readObject(in, LogicalTypeAnnotation.class);
    assertEquals(lta, lta1);
    out.reset();
    
    lta = LogicalTypeAnnotation.timeType(false, TimeUnit.NANOS);
    kryo.writeObject(out, lta);
    buf = out.getBuffer();
    in = new Input(buf);
    lta1 = kryo.readObject(in, LogicalTypeAnnotation.class);
    assertEquals(lta, lta1);
    out.reset();
    
    lta = LogicalTypeAnnotation.timestampType(true, TimeUnit.MICROS);
    kryo.writeObject(out, lta);
    buf = out.getBuffer();
    in = new Input(buf);
    lta1 = kryo.readObject(in, LogicalTypeAnnotation.class);
    assertEquals(lta, lta1);
    out.reset();
    
    lta = LogicalTypeAnnotation.timestampType(false, TimeUnit.MICROS);
    kryo.writeObject(out, lta);
    buf = out.getBuffer();
    in = new Input(buf);
    lta1 = kryo.readObject(in, LogicalTypeAnnotation.class);
    assertEquals(lta, lta1);
    out.reset();
    
    lta = LogicalTypeAnnotation.timestampType(true, TimeUnit.MILLIS);
    kryo.writeObject(out, lta);
    buf = out.getBuffer();
    in = new Input(buf);
    lta1 = kryo.readObject(in, LogicalTypeAnnotation.class);
    assertEquals(lta, lta1);
    out.reset();
    
    lta = LogicalTypeAnnotation.timestampType(false, TimeUnit.MILLIS);
    kryo.writeObject(out, lta);
    buf = out.getBuffer();
    in = new Input(buf);
    lta1 = kryo.readObject(in, LogicalTypeAnnotation.class);
    assertEquals(lta, lta1);
    out.reset();
    
    lta = LogicalTypeAnnotation.timestampType(true, TimeUnit.NANOS);
    kryo.writeObject(out, lta);
    buf = out.getBuffer();
    in = new Input(buf);
    lta1 = kryo.readObject(in, LogicalTypeAnnotation.class);
    assertEquals(lta, lta1);
    out.reset();
    
    lta = LogicalTypeAnnotation.timestampType(false, TimeUnit.NANOS);
    kryo.writeObject(out, lta);
    buf = out.getBuffer();
    in = new Input(buf);
    lta1 = kryo.readObject(in, LogicalTypeAnnotation.class);
    assertEquals(lta, lta1);
    out.reset();
    
    lta = LogicalTypeAnnotation.uuidType();
    kryo.writeObject(out, lta);
    buf = out.getBuffer();
    in = new Input(buf);
    lta1 = kryo.readObject(in, LogicalTypeAnnotation.class);
    assertEquals(lta, lta1);
    out.reset();
  }

}
