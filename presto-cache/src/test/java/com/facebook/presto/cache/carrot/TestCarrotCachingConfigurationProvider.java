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

import static com.facebook.presto.cache.CacheType.CARROT;
import static com.google.common.base.Preconditions.checkState;
import static java.nio.file.Files.createTempDirectory;
import static org.testng.Assert.assertEquals;

import java.io.IOException;
import java.net.URI;

import org.apache.hadoop.conf.Configuration;
import org.testng.annotations.AfterClass;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;

import com.carrot.cache.util.CarrotConfig;
import com.facebook.presto.cache.CacheConfig;


public class TestCarrotCachingConfigurationProvider {

  private URI cacheDirectory;
  private boolean validationEnabled = true;

  @BeforeClass
  public void setup()
          throws IOException
  {
      this.cacheDirectory = createTempDirectory("carrot_cache").toUri();
  }
  
  @AfterClass
  public void close()
          throws IOException
  {
      checkState(cacheDirectory != null);
      TestUtils.deletePathRecursively(cacheDirectory.getPath());
  }
  
  @Test(singleThreaded = true)
  public void testCarrotCachingConfigurationProvider() {
    CacheConfig cacheConfig = new CacheConfig()
        .setCacheType(CARROT)
        .setCachingEnabled(true)
        .setBaseDirectory(cacheDirectory)
        .setValidationEnabled(validationEnabled);
    CarrotCacheConfig carrotCacheConfig = new CarrotCacheConfig();
    Configuration configuration = TestUtils.getHdfsConfiguration(cacheConfig, carrotCacheConfig);
    
    String value = configuration.get(CarrotConfig.CACHE_MAXIMUM_SIZE_KEY);
    assertEquals(value, Long.toString(carrotCacheConfig.getMaxCacheSize().toBytes()));
    
    value = configuration.get(CarrotConfig.CACHE_SEGMENT_SIZE_KEY);
    assertEquals(value, Long.toString(carrotCacheConfig.getDataSegmentSize().toBytes()));
    
    value = configuration.get(CarrotConfig.CACHE_EVICTION_POLICY_IMPL_KEY);
    assertEquals(value, carrotCacheConfig.getEvictionPolicy().getClassName());
    
    value = configuration.get(CarrotConfig.CACHE_EVICTION_DISABLED_MODE_KEY);    
    assertEquals(value, Boolean.toString(carrotCacheConfig.isEvictionDisabled()));
    
    boolean aqEnabled = carrotCacheConfig.isCacheAdmissionControllerEnabled();
    if (aqEnabled) {
      
      value = configuration.get(CarrotConfig.CACHE_ADMISSION_CONTROLLER_IMPL_KEY);
      assertEquals(value, "com.carrot.cache.controllers.AQBasedAdmissionController");
      value = configuration.get(CarrotConfig.ADMISSION_QUEUE_START_SIZE_RATIO_KEY);
      assertEquals(value, Double.toString(carrotCacheConfig.getCacheAdmissionQueueSizeRatio()));
    }
    EvictionPolicy policy = carrotCacheConfig.getEvictionPolicy();
    assertEquals(configuration.get(CarrotConfig.CACHE_EVICTION_POLICY_IMPL_KEY), policy.getClassName());
    
    if (policy == EvictionPolicy.SLRU) {
      value = configuration.get(CarrotConfig.SLRU_CACHE_INSERT_POINT_KEY);
      assertEquals(value, Integer.toString(carrotCacheConfig.getSLRUEvictionInsertionPoint()));
    }
    
    value = configuration.get(CarrotConfig.SCAVENGER_DUMP_ENTRY_BELOW_START_KEY); 
    assertEquals(value, Double.toString(carrotCacheConfig.getCacheScavengerDumpEntryBelow()));
    
    value = configuration.get(CarrotConfig.SCAVENGER_START_RUN_RATIO_KEY);
    assertEquals(value, Double.toString(carrotCacheConfig.getCacheScavengerStartUsageRatio()));
    
    value = configuration.get(CarrotConfig.SCAVENGER_STOP_RUN_RATIO_KEY);
    assertEquals(value, Double.toString(carrotCacheConfig.getCacheScavengerStopUsageRatio()));
    
    value = configuration.get(CarrotConfig.CACHES_NAME_LIST_KEY);
    assertEquals(value, "data-cache");
    value = configuration.get(CarrotConfig.CACHES_TYPES_LIST_KEY);
    assertEquals(value, "file");
    
    value = configuration.get("cache.carrot.data-page-size"); 
    assertEquals(value, Long.toString(carrotCacheConfig.getCachePageSize().toBytes()));
    
    value = configuration.get("cache.carrot.io-buffer-size");
    assertEquals(value, Long.toString(carrotCacheConfig.getIOBufferSize().toBytes()));
    
    value = configuration.get("cache.carrot.io-pool-size");
    assertEquals(value, Long.toString(carrotCacheConfig.getIOPoolSize()));
    
  }
  
}
