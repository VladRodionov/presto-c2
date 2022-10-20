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

import static io.airlift.units.DataSize.Unit.GIGABYTE;

import com.facebook.airlift.configuration.Config;
import com.facebook.airlift.configuration.ConfigDescription;

import io.airlift.units.DataSize;
import io.airlift.units.DataSize.Unit;

public class CarrotCacheConfig
{
  
    public static String CACHE_NAME = "data-cache";
    
    private boolean metricsCollectionEnabled = false;
    private String metricsDomain = "com.facebook.carrot";
    private DataSize maxCacheSize = new DataSize(500, GIGABYTE);
    private DataSize dataSegmentSize = new DataSize(128, Unit.MEGABYTE);
    private DataSize cachePageSize = new DataSize(1, Unit.MEGABYTE);
    private DataSize ioBufferSize = new DataSize(256, Unit.KILOBYTE);
    private int ioPoolSize = 32;
    private boolean configValidationEnabled = false;
    private boolean cacheQuotaEnabled = false;
    // If eviction is disabled
    private boolean evictionDisabled = false;
    // If cache admission enabled
    private boolean cacheAdmissionControllerEnabled = true;
    // Virtual cache admission queue size as a fraction of the cache size 
    private double cacheAdmissionQueueSizeRatio = 0.5;
    // All entries whose popularity is below this value will be discarded during GC
    private double cacheScavengerDumpEntryBelow = 0.5;
    // Garbage collection starts if storage usage exceeds this ratio
    private double cacheScavengerStartUsageRatio = 0.95;
    // Garbage collection stops if storage usage falls below this ratio
    private double cacheScavengerStopUsageRatio = 0.90;
    // SLRU eviction policy new item insertion point (0 - 7) when 0 - its LRU
    private int evictionSLRUInsertionPoint = 5;
    // Start index of slot power ( 2 ** x  - initial size of the hash-table )
    private int startIndexNumberSlotsPower = 10;
    // Eviction policy for the cache
    private EvictionPolicy evictionPolicy = EvictionPolicy.SLRU;
    // Recycling selector for GC
    private RecyclingSelector recyclingSelector = RecyclingSelector.LRC;
    // Minimum active data set ration - set it to 0.
    private double minActiveDatasetRatio = 0.9;

    public boolean isMetricsCollectionEnabled()
    {
        return metricsCollectionEnabled;
    }

    @Config("cache.carrot.metrics-enabled")
    @ConfigDescription("If metrics for carrot caching are enabled")
    public CarrotCacheConfig setMetricsCollectionEnabled(boolean metricsCollectionEnabled)
    {
        this.metricsCollectionEnabled = metricsCollectionEnabled;
        return this;
    }

    public String getMetricsDomain()
    {
        return metricsDomain;
    }

    @Config("cache.carrot.metrics-domain")
    @ConfigDescription("Metrics domain name used by the carrot caching")
    public CarrotCacheConfig setMetricsDomain(String metricsDomain)
    {
        this.metricsDomain = metricsDomain;
        return this;
    }

    public DataSize getCachePageSize()
    {
        return cachePageSize;
    }

    @Config("cache.carrot.data-page-size")
    @ConfigDescription("Data page size")
    public CarrotCacheConfig setCachePageSize(DataSize size)
    {
        this.cachePageSize = size;
        return this;
    }

    public DataSize getIOBufferSize()
    {
        return ioBufferSize;
    }

    @Config("cache.carrot.io-buffer-size")
    @ConfigDescription("I/O buffer size")
    public CarrotCacheConfig setIOBufferSize(DataSize size)
    {
        this.ioBufferSize = size;
        return this;
    }
    
    public DataSize getMaxCacheSize()
    {
        return maxCacheSize;
    }

    @Config("cache.carrot.max-cache-size")
    @ConfigDescription("The maximum cache size available for carrot cache")
    public CarrotCacheConfig setMaxCacheSize(DataSize maxCacheSize)
    {
        this.maxCacheSize = maxCacheSize;
        return this;
    }
    
    public DataSize getDataSegmentSize()
    {
        return dataSegmentSize;
    }

    @Config("cache.carrot.data-segment-size")
    @ConfigDescription("The data segment size")
    public CarrotCacheConfig setDataSegmentSize(DataSize size)
    {
        this.dataSegmentSize = size;
        return this;
    }
    
    public boolean isConfigValidationEnabled()
    {
        return configValidationEnabled;
    }
    
    @Config("cache.carrot.config-validation-enabled")
    @ConfigDescription("If the carrot caching should validate the provided configuration")
    public CarrotCacheConfig setConfigValidationEnabled(boolean configValidationEnabled)
    {
        this.configValidationEnabled = configValidationEnabled;
        return this;
    }

    public boolean isEvictionDisabled()
    {
        return evictionDisabled;
    }

    @Config("cache.carrot.eviction-disabled")
    @ConfigDescription("If the carrot caching should evict data")
    public CarrotCacheConfig setEvictionDisabled(boolean b)
    {
        this.evictionDisabled = b;
        return this;
    }
    
    public EvictionPolicy getEvictionPolicy()
    {
        return evictionPolicy;
    }

    @Config("cache.carrot.eviction-policy")
    @ConfigDescription("The cache eviction policy")
    public CarrotCacheConfig setEvictionPolicy(EvictionPolicy evictionPolicy)
    {
        this.evictionPolicy = evictionPolicy;
        return this;
    }

    public RecyclingSelector getRecyclingSelector()
    {
        return recyclingSelector;
    }

    @Config("cache.carrot.recycling-selector")
    @ConfigDescription("The cache recycling selector")
    public CarrotCacheConfig setRecyclingSelector(RecyclingSelector recyclingSelector)
    {
        this.recyclingSelector = recyclingSelector;
        return this;
    }
    
    public boolean isCacheQuotaEnabled()
    {
        return cacheQuotaEnabled;
    }

    @Config("cache.carrot.quota-enabled")
    @ConfigDescription("Whether to enable quota for carrot caching operations")
    public CarrotCacheConfig setCacheQuotaEnabled(boolean cacheQuotaEnabled)
    {
        this.cacheQuotaEnabled = cacheQuotaEnabled;
        return this;
    }

    public boolean isCacheAdmissionControllerEnabled()
    {
        return cacheAdmissionControllerEnabled;
    }

    @Config("cache.carrot.admission-controller-enabled")
    @ConfigDescription("Whether to enable quota for carrot caching operations")
    public CarrotCacheConfig setCacheAdmissionControllerEnabled(boolean b)
    {
        this.cacheAdmissionControllerEnabled = b;
        return this;
    }
    
    public double getCacheAdmissionQueueSizeRatio() {
      return this.cacheAdmissionQueueSizeRatio;
    }
    
    @Config("cache.carrot.admission-queue-size-ratio")
    @ConfigDescription("Size of the admission queue as a ratio of a maximum cache size")
    public CarrotCacheConfig setCacheAdmissionQueueSizeRatio(double v)
    {
        this.cacheAdmissionQueueSizeRatio = v;
        return this;
    }
    
    public double getCacheScavengerDumpEntryBelow() {
      return this.cacheScavengerDumpEntryBelow;
    }
    
    @Config("cache.carrot.scavenger-dump-entry-below")
    @ConfigDescription("Dump entry during Garbage Collection if its popularity is below this value")
    public CarrotCacheConfig setCacheScavengerDumpEntryBelow(double v)
    {
        this.cacheScavengerDumpEntryBelow = v;
        return this;
    }
    
    public double getCacheScavengerStartUsageRatio() {
      return this.cacheScavengerStartUsageRatio;
    }
    
    @Config("cache.carrot.scavenger-start-usage-ratio")
    @ConfigDescription("Start Garbage Collection when storage usage exceeds this ratio")
    public CarrotCacheConfig setCacheScavengerStartUsageRatio(double v)
    {
        this.cacheScavengerStartUsageRatio = v;
        return this;
    }
    
    public double getCacheScavengerStopUsageRatio() {
      return this.cacheScavengerStopUsageRatio;
    }
    
    @Config("cache.carrot.scavenger-stop-usage-ratio")
    @ConfigDescription("Start Garbage Collection when storage usage falls below this ratio")
    public CarrotCacheConfig setCacheScavengerStopUsageRatio(double v)
    {
        this.cacheScavengerStopUsageRatio = v;
        return this;
    }
    
    public int getSLRUEvictionInsertionPoint() {
      return this.evictionSLRUInsertionPoint;
    }
    
    @Config("cache.carrot.eviction.slru-insertion-point")
    @ConfigDescription("SLRU eviction policy insertion point")
    public CarrotCacheConfig setSLRUEvictionInsertionPoint(int v)
    {
        this.evictionSLRUInsertionPoint = v;
        return this;
    }
    
    public int getStartIndexOfSlotsPower() {
      return this.startIndexNumberSlotsPower;
    }
    
    @Config("cache.carrot.start-index-power")
    @ConfigDescription("Initial size of index hash table")
    public CarrotCacheConfig setStartIndexSlotsPower(int power) {
      this.startIndexNumberSlotsPower = power;
      return this;
    }
    
    public double getMinActiveDatasetRatio() {
      return this.minActiveDatasetRatio;
    }
    
    @Config("cache.carrot.min-active-dataset-ratio")
    @ConfigDescription("Run Garbage Collection when ratio of 'alive' objects to the total size falls below this ratio")
    public CarrotCacheConfig setMinActiveDatasetRatio(double v)
    {
        this.minActiveDatasetRatio = v;
        return this;
    }
    
    public int getIOPoolSize()
    {
        return ioPoolSize;
    }

    @Config("cache.carrot.io-pool-size")
    @ConfigDescription("I/O pool size")
    public CarrotCacheConfig setIOPoolSize(int size)
    {
        this.ioPoolSize = size;
        return this;
    }
}
