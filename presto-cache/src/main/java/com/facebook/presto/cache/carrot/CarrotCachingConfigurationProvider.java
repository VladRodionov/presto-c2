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


import com.carrot.cache.controllers.AQBasedAdmissionController;
import com.carrot.cache.util.CarrotConfig;
import com.facebook.presto.cache.CacheConfig;
import com.facebook.presto.hive.DynamicConfigurationProvider;
import com.facebook.presto.hive.HdfsContext;

import org.apache.hadoop.conf.Configuration;

import javax.inject.Inject;

import java.net.URI;

import static com.facebook.presto.cache.CacheType.CARROT;
import static com.facebook.presto.cache.carrot.CarrotCacheConfig.toCachePropertyName;
import static com.facebook.presto.cache.carrot.CarrotCacheConfig.DATA_PAGE_SIZE;
import static com.facebook.presto.cache.carrot.CarrotCacheConfig.IO_BUFFER_SIZE;
import static com.facebook.presto.cache.carrot.CarrotCacheConfig.IO_POOL_SIZE;


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
          // All cache instances in the application must have the same base directory 
          configuration.set(CarrotConfig.CACHE_ROOT_DIR_PATH_KEY,
            cacheConfig.getBaseDirectory().getPath());
        } else {
          throw new RuntimeException("Failed to initialize data page cache: 'cache.base-directory' is not set");
        }
        
        configuration.set(toCachePropertyName(CarrotConfig.CACHE_MAXIMUM_SIZE_KEY),
          Long.toString(carrotCacheConfig.getMaxCacheSize().toBytes()));
        configuration.set(toCachePropertyName(CarrotConfig.CACHE_SEGMENT_SIZE_KEY),
          Long.toString(carrotCacheConfig.getDataSegmentSize().toBytes()));
        configuration.set(toCachePropertyName(CarrotConfig.CACHE_EVICTION_POLICY_IMPL_KEY),
          carrotCacheConfig.getEvictionPolicy().getClassName());
        configuration.set(toCachePropertyName(CarrotConfig.CACHE_EVICTION_DISABLED_MODE_KEY),
          Boolean.toString(carrotCacheConfig.isEvictionDisabled()));
        boolean aqEnabled = carrotCacheConfig.isCacheAdmissionControllerEnabled();
        if (aqEnabled) {
          configuration.set(toCachePropertyName(CarrotConfig.CACHE_ADMISSION_CONTROLLER_IMPL_KEY),
             AQBasedAdmissionController.class.getName());
          configuration.set(toCachePropertyName(CarrotConfig.ADMISSION_QUEUE_START_SIZE_RATIO_KEY),
            Double.toString(carrotCacheConfig.getCacheAdmissionQueueSizeRatio()));
        }
        EvictionPolicy policy = carrotCacheConfig.getEvictionPolicy();
        configuration.set(toCachePropertyName(CarrotConfig.CACHE_EVICTION_POLICY_IMPL_KEY), policy.getClassName());
        RecyclingSelector selector = carrotCacheConfig.getRecyclingSelector();
        configuration.set(toCachePropertyName(CarrotConfig.CACHE_RECYCLING_SELECTOR_IMPL_KEY), selector.getClassName());
        if (policy == EvictionPolicy.SLRU) {
          configuration.set(toCachePropertyName(CarrotConfig.SLRU_CACHE_INSERT_POINT_KEY),
            Integer.toString(carrotCacheConfig.getSLRUEvictionInsertionPoint()));
        }
        configuration.set(toCachePropertyName(CarrotConfig.SCAVENGER_DUMP_ENTRY_BELOW_MIN_KEY), 
          Double.toString(carrotCacheConfig.getCacheScavengerDumpEntryBelow()));
        configuration.set(toCachePropertyName(CarrotConfig.SCAVENGER_START_RUN_RATIO_KEY), 
          Double.toString(carrotCacheConfig.getCacheScavengerStartUsageRatio()));
        configuration.set(toCachePropertyName(CarrotConfig.SCAVENGER_STOP_RUN_RATIO_KEY), 
          Double.toString(carrotCacheConfig.getCacheScavengerStopUsageRatio()));
        
        configuration.set(toCachePropertyName(CarrotConfig.START_INDEX_NUMBER_OF_SLOTS_POWER_KEY), 
          Integer.toString(carrotCacheConfig.getStartIndexSlotsPower()));
        configuration.set(toCachePropertyName(CarrotConfig.CACHE_MINIMUM_ACTIVE_DATA_SET_RATIO_KEY), 
          Double.toString(carrotCacheConfig.getMinActiveDatasetRatio()));
        
        addCacheToBaseConfig();
        
        configuration.set("cache.carrot.metrics-enabled", 
          Boolean.toString(carrotCacheConfig.isMetricsCollectionEnabled()));
        if(carrotCacheConfig.isMetricsCollectionEnabled()) {
          configuration.set(toCachePropertyName(CarrotConfig.CACHE_JMX_METRICS_DOMAIN_NAME_KEY), 
            carrotCacheConfig.getMetricsDomain());
        }
        configuration.set(DATA_PAGE_SIZE, Long.toString(carrotCacheConfig.getCachePageSize().toBytes()));
        configuration.set(IO_BUFFER_SIZE, Long.toString(carrotCacheConfig.getIOBufferSize().toBytes()));
        configuration.set(IO_POOL_SIZE, Long.toString(carrotCacheConfig.getIOPoolSize()));
      }
    }
    
    private void addCacheToBaseConfig() {
      CarrotConfig config = CarrotConfig.getInstance();
      String cacheName = CarrotCacheConfig.CACHE_NAME;
      String type = "file";
      String[] names = config.getCacheNames();
      String[] types = config.getCacheTypes();
      
      String[] newNames = new String[names.length + 1];
      System.arraycopy(names, 0, newNames, 0, names.length);
      newNames[newNames.length - 1] = cacheName;
      String[] newTypes = new String[types.length + 1];
      System.arraycopy(types, 0, newTypes, 0, types.length);
      newTypes[newTypes.length - 1] = type;
      String cacheNames = String.join(",", newNames);
      String cacheTypes = String.join(",", newTypes);
      config.setCacheNames(cacheNames);
      config.setCacheTypes(cacheTypes);
    }
    
    @Override
    public boolean isUriIndependentConfigurationProvider()
    {
      // All the config set above are independent of the URI
      //TODO: is it right?  
      return true;
    }
}
