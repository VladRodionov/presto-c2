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

import com.carrot.cache.util.CarrotConfig;
import com.facebook.presto.cache.CacheConfig;
import com.facebook.presto.hive.DynamicConfigurationProvider;
import com.facebook.presto.hive.HdfsContext;
import org.apache.hadoop.conf.Configuration;

import javax.inject.Inject;

import java.net.URI;

import static com.facebook.presto.cache.CacheType.CARROT;

public class CarrotCachingConfigurationProvider
        implements DynamicConfigurationProvider
{
    private final CacheConfig cacheConfig;
    private final CarrotCacheConfig carrotCacheConfig;

    @Inject
    public CarrotCachingConfigurationProvider(CacheConfig cacheConfig, CarrotCacheConfig carrotCacheConfig)
    {
        this.cacheConfig = cacheConfig;
        this.carrotCacheConfig = carrotCacheConfig;
    }

    @Override
    public void updateConfiguration(Configuration configuration, HdfsContext context, URI uri) {
      if (cacheConfig.isCachingEnabled() && cacheConfig.getCacheType() == CARROT) {
        if (cacheConfig.getBaseDirectory() != null) {
          configuration.set(CarrotConfig.CACHE_ROOT_DIR_PATH_KEY,
            cacheConfig.getBaseDirectory().getPath());
        }

        configuration.set(CarrotConfig.CACHE_MAXIMUM_SIZE_KEY,
          Long.toString(carrotCacheConfig.getMaxCacheSize().toBytes()));
        configuration.set(CarrotConfig.CACHE_SEGMENT_SIZE_KEY,
          Long.toString(carrotCacheConfig.getDataSegmentSize().toBytes()));
        configuration.set(CarrotConfig.CACHE_EVICTION_POLICY_IMPL_KEY,
          carrotCacheConfig.getEvictionPolicy().getClassName());
        configuration.set(CarrotConfig.CACHE_EVICTION_DISABLED_MODE_KEY,
          Boolean.toString(carrotCacheConfig.isEvictionDisabled()));
        boolean aqEnabled = carrotCacheConfig.isCacheAdmissionControllerEnabled();
        if (aqEnabled) {
          configuration.set(CarrotConfig.CACHE_ADMISSION_CONTROLLER_IMPL_KEY,
            "com.carrot.cache.controllers.AQBasedAdmissionController");
          configuration.set(CarrotConfig.ADMISSION_QUEUE_START_SIZE_RATIO_KEY,
            Double.toString(carrotCacheConfig.getCacheAdmissionQueueSizeRatio()));
        }
        EvictionPolicy policy = carrotCacheConfig.getEvictionPolicy();
        configuration.set(CarrotConfig.CACHE_EVICTION_POLICY_IMPL_KEY, policy.getClassName());
        RecyclingSelector selector = carrotCacheConfig.getRecyclingSelector();
        configuration.set(CarrotConfig.CACHE_RECYCLING_SELECTOR_IMPL_KEY, selector.getClassName());
        if (policy == EvictionPolicy.SLRU) {
          configuration.set(CarrotConfig.SLRU_CACHE_INSERT_POINT_KEY,
            Integer.toString(carrotCacheConfig.getSLRUEvictionInsertionPoint()));
        }
        configuration.set(CarrotConfig.SCAVENGER_DUMP_ENTRY_BELOW_START_KEY, 
          Double.toString(carrotCacheConfig.getCacheScavengerDumpEntryBelow()));
        configuration.set(CarrotConfig.SCAVENGER_START_RUN_RATIO_KEY, 
          Double.toString(carrotCacheConfig.getCacheScavengerStartUsageRatio()));
        configuration.set(CarrotConfig.SCAVENGER_STOP_RUN_RATIO_KEY, 
          Double.toString(carrotCacheConfig.getCacheScavengerStopUsageRatio()));
        
        configuration.set(CarrotConfig.START_INDEX_NUMBER_OF_SLOTS_POWER_KEY, 
          Integer.toString(carrotCacheConfig.getStartIndexSlotsPower()));
        configuration.set(CarrotConfig.CACHE_MINIMUM_ACTIVE_DATA_SET_RATIO_KEY, 
          Double.toString(carrotCacheConfig.getMinActiveDatasetRatio()));
        //FIXME - this works only for single cache
        configuration.set(CarrotConfig.CACHES_NAME_LIST_KEY, CarrotCacheConfig.CACHE_NAME);
        configuration.set(CarrotConfig.CACHES_TYPES_LIST_KEY, "file");
        configuration.set("cache.carrot.metrics-enabled", 
          Boolean.toString(carrotCacheConfig.isMetricsCollectionEnabled()));
        if(carrotCacheConfig.isMetricsCollectionEnabled()) {
          configuration.set(CarrotConfig.CACHE_JMX_METRICS_DOMAIN_NAME_KEY, carrotCacheConfig.getMetricsDomain());
        }
        configuration.set("cache.carrot.data-page-size", Long.toString(carrotCacheConfig.getCachePageSize().toBytes()));
        configuration.set("cache.carrot.io-buffer-size", Long.toString(carrotCacheConfig.getIOBufferSize().toBytes()));
        configuration.set("cache.carrot.io-pool-size", Long.toString(carrotCacheConfig.getIOPoolSize()));
        
      }
    }

    @Override
    public boolean isUriIndependentConfigurationProvider()
    {
        // All the config set above are independent of the URI
        return true;
    }
}
