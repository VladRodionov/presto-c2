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
package com.facebook.presto.cache.carrot;

public enum EvictionPolicy
{
    FIFO("com.carrot.cache.eviction.FIFOEvictionPolicy"),
    SLRU("com.carrot.cache.eviction.SLRUEvictionPolicy"),
    LRU("com.carrot.cache.eviction.LRUEvictionPolicy");

    private final String className;

    EvictionPolicy(String className)
    {
        this.className = className;
    }

    public String getClassName()
    {
        return className;
    }
}
