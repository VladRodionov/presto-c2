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

import java.util.Arrays;
import java.util.Random;

import com.facebook.presto.orc.metadata.statistics.HiveBloomFilter;

public class TestUtils {

  public static HiveBloomFilter getBloomFilter() {
    long[] bits = new long[100];
    int numBits = bits.length * 64;
    int numHashes = 5;
    HiveBloomFilter filter = new HiveBloomFilter(bits, numBits, numHashes);
    Random r = new Random();
    for (int i = 0; i < 100; i++) {
      long v = r.nextLong();
      filter.addLong(v);
    }
    return filter;
  }
  
  public static boolean equals(HiveBloomFilter f1, HiveBloomFilter f2) {
    if (f1 == null || f2 == null) {
      return f1 == null && f2 == null;
    }
    return f1.getBitSize() == f2.getBitSize() && f1.getNumHashFunctions() == f2.getNumHashFunctions() &&
        Arrays.equals(f1.getBitSet(), f2.getBitSet());
  }
}
