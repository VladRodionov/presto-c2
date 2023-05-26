package com.facebook.presto.orc.serde;

import static org.testng.Assert.assertEquals;

import org.testng.annotations.Test;

import com.esotericsoftware.kryo.kryo5.io.Output;
import com.facebook.presto.orc.OrcDataSourceId;

public class TestOrcDataSourceIdSerializer extends TestSerializerBase {

  @Test
  public void testOrcDataSourceIdSerializer() {
    OrcDataSourceId id = new OrcDataSourceId("wwwwwwww");
    Output out = new Output(100);
    OrcDataSourceIdSerializer serde = new OrcDataSourceIdSerializer();
    serde.write(kryo, out, id);
    assertEquals(16, out.position());
    
  }
}
