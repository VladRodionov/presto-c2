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

import org.testng.annotations.Test;

import com.esotericsoftware.kryo.kryo5.io.Output;
import com.facebook.presto.orc.OrcDataSourceId;
import com.facebook.presto.orc.StripeReader.StripeId;

public class TestStripeIdSerializer extends TestSerializerBase {

  @Test
  public void testStripeIdSerializer() {
    OrcDataSourceId id = new OrcDataSourceId("wwwwwwww");

    StripeId sid = new StripeId(id, 100);
    Output out = new Output(100);
    StripeIdSerializer serde = new StripeIdSerializer();
    serde.write(kryo, out, sid);
    assertEquals(16, out.position());
  }

 
}
