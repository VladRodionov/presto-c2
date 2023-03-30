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
package com.facebook.presto.parquet.cache;

import com.facebook.airlift.configuration.Config;
import com.facebook.airlift.configuration.ConfigDescription;

import io.airlift.units.DataSize;
import io.airlift.units.DataSize.Unit;
import io.airlift.units.Duration;
import io.airlift.units.MinDataSize;
import io.airlift.units.MinDuration;

import static io.airlift.units.DataSize.Unit.BYTE;
import static java.util.concurrent.TimeUnit.SECONDS;

import java.net.URI;

public class ParquetCacheConfig
{
    private boolean metadataCacheEnabled;
    private DataSize metadataCacheSize = new DataSize(0, BYTE);
    private Duration metadataCacheTtlSinceLastAccess = new Duration(0, SECONDS);
    private ParquetMetadataCacheType cacheType = ParquetMetadataCacheType.GUAVA;
    
    /****************************************** 
     * 
     * Carrot configuration section 
     * 
     ******************************************/
    
    /** 
     * 
     * Used when cache type is CARROT_HYBRID 
     * We need to have separate settings for OFFHEAP and FILE caches
     * 
     **/
    private DataSize carrotOffheapMaxCacheSize = new DataSize(0, Unit.MEGABYTE);
    
    /**
     * Carrot cache maximum size in CARROT_HYBRID mode
     */
    private DataSize carrotFileMaxCacheSize = new DataSize(0, Unit.MEGABYTE);
    
    private DataSize carrotOffheapDataSegmentSize = new DataSize(64, Unit.MEGABYTE);
    
    private DataSize carrotFileDataSegmentSize = new DataSize(256, Unit.MEGABYTE);
    
    private URI carrotCacheRootDir;
    
    private boolean carrotJMXMetricsEnabled;
    
    private String carrotJMXDomainName;
    
    private boolean carrotSaveOnShutdown = true;
    
    /**
     * Used for cache type CARROT_FILE (CARROT_HYBRID)
     */
    private boolean carrotAdmissionControllerEnabled = false;
    
    /**
     * Used for cache type CARROT_FILE (CARROT_HYBRID)
     */
    private double carrotAdmissionControllerRatio;

    public boolean isMetadataCacheEnabled()
    {
        return metadataCacheEnabled;
    }
    
    @Config("parquet.metadata-cache-type")
    @ConfigDescription("Parquet metadata cache type")
    public ParquetCacheConfig setParquetMetadataCacheType(ParquetMetadataCacheType type) {
      this.cacheType = type;
      return this;
    }
    
    public ParquetMetadataCacheType getParquetMetadataCacheType() {
      return this.cacheType;
    }

    @Config("parquet.metadata-cache-enabled")
    @ConfigDescription("Enable cache for parquet metadata")
    public ParquetCacheConfig setMetadataCacheEnabled(boolean metadataCacheEnabled)
    {
        this.metadataCacheEnabled = metadataCacheEnabled;
        return this;
    }

    @MinDataSize("0B")
    public DataSize getMetadataCacheSize()
    {
        return metadataCacheSize;
    }

    @Config("parquet.metadata-cache-size")
    @ConfigDescription("Size of the parquet metadata cache")
    public ParquetCacheConfig setMetadataCacheSize(DataSize metadataCacheSize)
    {
        this.metadataCacheSize = metadataCacheSize;
        return this;
    }

    @MinDuration("0s")
    public Duration getMetadataCacheTtlSinceLastAccess()
    {
        return metadataCacheTtlSinceLastAccess;
    }

    @Config("parquet.metadata-cache-ttl-since-last-access")
    @ConfigDescription("Time-to-live for parquet metadata cache entry after last access")
    public ParquetCacheConfig setMetadataCacheTtlSinceLastAccess(Duration metadataCacheTtlSinceLastAccess)
    {
        this.metadataCacheTtlSinceLastAccess = metadataCacheTtlSinceLastAccess;
        return this;
    }
    
    /**
     * Carrot cache configuration section
     */
    public DataSize getCarrotOffheapDataSegmentSize()
    {
        return this.carrotOffheapDataSegmentSize;
    }

    @Config("parquet.carrot.file-data-segment-size")
    @ConfigDescription("The data segment size for file cache")
    public ParquetCacheConfig setCarrotFileDataSegmentSize(DataSize size)
    {
        this.carrotFileDataSegmentSize = size;
        return this;
    }
    
    /**
     * Get carrot file cache segment size
     */
    public DataSize getCarrotFileDataSegmentSize()
    {
        return this.carrotFileDataSegmentSize;
    }
    
    @MinDataSize("0B")
    public DataSize getCarrotOffheapCacheSize()
    {
        return this.carrotOffheapMaxCacheSize;
    }

    @Config("parquet.carrot-offheap-cache-size")
    @ConfigDescription("Size of the parquet metadata carrot offheap cache")
    public ParquetCacheConfig setCarrotOffheapMaxCacheSize(DataSize offheapCacheSize)
    {
      this.carrotOffheapMaxCacheSize = offheapCacheSize;
        return this;
    }
    @MinDataSize("0B")
    public DataSize getCarrotFileCacheSize()
    {
        return this.carrotFileMaxCacheSize;
    }

    @Config("parquet.carrot-file-cache-size")
    @ConfigDescription("Size of the parquet metadata carror file cache")
    public ParquetCacheConfig setCarrotFileMaxCacheSize(DataSize fileCacheSize)
    {
      this.carrotFileMaxCacheSize = fileCacheSize;
      return this;
    }
    /**
     * Set carrot file cache segment size
     * @param size segment size
     * @return self
     */
    @Config("parquet.carrot.offheap-data-segment-size")
    @ConfigDescription("The data segment size for offheap cache")
    public ParquetCacheConfig setCarrotOffheapDataSegmentSize(DataSize size)
    {
        this.carrotOffheapDataSegmentSize = size;
        return this;
    }
    
    /**
     * Get carrot cache root directory
     * @return root directory
     */
    public URI getCarrotCacheRootDir() {
      return this.carrotCacheRootDir;
    }
    
    /**
     * Set carrot cache root directory
     * @param dir root directory
     * @return self
     */
    @Config("parquet.carrot.cache-root-dir")
    public ParquetCacheConfig setCarrotCacheRootDir(URI dir) {
      this.carrotCacheRootDir = dir;
      return this;
    }
    
    /**
     * Is JMX metrics enabled
     * @return true or false
     */
    public boolean isCarrotJMXMetricsEnabled() {
      return carrotJMXMetricsEnabled;
    }
    
    /**
     * Set carrot JMX metrics enabled
     * @param b true or false
     * @return self
     */
    @Config("parquet.carrot.jmx-metrics-enabled")
    public ParquetCacheConfig setCarrotJMXMetricsEnabled(boolean b) {
      this.carrotJMXMetricsEnabled = b;
      return this;
    }
    
    /**
     * Save cache on shutdown
     * @return true or false
     */
    public boolean getCarrotCacheSaveOnShutdown() {
      return this.carrotSaveOnShutdown;
    }
    
    /**
     * Set carrot cache save on shutdown
     * @param b true or false
     * @return self
     */
    @Config("parquet.carrot.cache-save-on-shutdown")
    public ParquetCacheConfig setCarrotCacheSaveOnShutdown(boolean b) {
      this.carrotSaveOnShutdown = b;
      return this;
    }
    
    /**
     * Get Carrot JMX domain name 
     * @return domain name
     */
    public String getCarrotJMXDomainName() {
      return this.carrotJMXDomainName;
    }
    
    /**
     * Set carrot JMX domain name
     * @param domainName domain name
     * @return self
     */
    @Config("parquet.carrot.jmx-domain-name")
    public ParquetCacheConfig setCarrotJMXDomainName(String domainName) {
      this.carrotJMXDomainName = domainName;
      return this;
    }
    
    /**
     * Is Carrot admission controller enabled
     * @return true or false
     */
    public boolean isCarrotAdmissionControllerEnabled() {
      return this.carrotAdmissionControllerEnabled;
    }
    
    /**
     * Set carrot admission controller enabled for file cache
     * @param b true or false
     * @return self
     */
    @Config("parquet.carrot.admission-controller-enabled")
    public ParquetCacheConfig setCarrotAdmissionControllerEnabled(boolean b) {
      this.carrotAdmissionControllerEnabled = b;
      return this;
    }
    
    /**
     * Get carrot cache admission controller ratio
     * @return ratio
     */
    public double getCarrotAdmissionControllerRatio() {
      return this.carrotAdmissionControllerRatio;
    }
    
    /**
     *  Set Carrot admission controller ratio
     * @param r ratio
     * @return self
     */
    @Config("parquet.carrot.admission-controller-ratio")
    public ParquetCacheConfig setCarrotAdmissionControllerRatio(double r) {
      this.carrotAdmissionControllerRatio = r;
      return this;
    }
}
