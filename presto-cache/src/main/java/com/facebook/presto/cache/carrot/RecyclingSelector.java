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

public enum RecyclingSelector
{
    // Least Recently Created data segment to recycle
    LRC("com.carrot.cache.controllers.LRCRecyclingSelector"),
    // Minimum alive objects data segment to recycle
    MIN_ALIVE("com.carrot.cache.controllers.MinAliveRecyclingSelector");

    private final String className;

    RecyclingSelector(String className)
    {
        this.className = className;
    }

    public String getClassName()
    {
        return className;
    }
}
