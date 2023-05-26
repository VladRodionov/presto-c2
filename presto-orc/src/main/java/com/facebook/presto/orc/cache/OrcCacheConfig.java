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
package com.facebook.presto.orc.cache;

import com.facebook.airlift.configuration.Config;
import com.facebook.airlift.configuration.ConfigDescription;

import io.airlift.units.DataSize;
import io.airlift.units.Duration;
import io.airlift.units.MinDataSize;
import io.airlift.units.MinDuration;
import io.airlift.units.DataSize.Unit;

import static com.facebook.presto.orc.OrcDataSourceUtils.EXPECTED_FOOTER_SIZE_IN_BYTES;
import static io.airlift.units.DataSize.Unit.BYTE;
import static java.util.concurrent.TimeUnit.SECONDS;

import java.net.URI;

public class OrcCacheConfig
{
    
    private OrcMetadataCacheType metadataCacheType = OrcMetadataCacheType.GUAVA;
    
    private boolean fileTailCacheEnabled;
    private DataSize fileTailCacheSize = new DataSize(0, BYTE);
    private Duration fileTailCacheTtlSinceLastAccess = new Duration(0, SECONDS);

    private boolean stripeMetadataCacheEnabled;
    private DataSize stripeFooterCacheSize = new DataSize(0, BYTE);
    private Duration stripeFooterCacheTtlSinceLastAccess = new Duration(0, SECONDS);
    private DataSize stripeStreamCacheSize = new DataSize(0, BYTE);
    private Duration stripeStreamCacheTtlSinceLastAccess = new Duration(0, SECONDS);

    private boolean rowGroupIndexCacheEnabled;
    private DataSize rowGroupIndexCacheSize = new DataSize(0, BYTE);
    private Duration rowGroupIndexCacheTtlSinceLastAccess = new Duration(0, SECONDS);

    private boolean dwrfStripeCacheEnabled;
    private DataSize expectedFileTailSize = new DataSize(EXPECTED_FOOTER_SIZE_IN_BYTES, BYTE);

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
    private double carrotAdmissionControllerRatio = 0.5;

    
    public OrcMetadataCacheType getMetadataCacheType() {
      return metadataCacheType;
    }
    
    @Config("orc.metadata-cache-type")
    @ConfigDescription("Orc metadata cache type: GUAVA (default), CARROT_OFFHEAP, CARROT_FILE, CARROT_HYBRID")
    public OrcCacheConfig setMetadataCacheType(OrcMetadataCacheType cacheType) {
      metadataCacheType = cacheType;
      return this;
    }
    
    /*************************************
     * 
     * Carrot cache configuration section
     * 
     *************************************/
    public DataSize getCarrotOffheapDataSegmentSize()
    {
        return this.carrotOffheapDataSegmentSize;
    }
    
    /**
     * Set carrot file cache segment size
     * @param size segment size
     * @return self
     */
    @Config("orc.carrot-offheap-data-segment-size")
    @ConfigDescription("The data segment size for offheap cache")
    public OrcCacheConfig setCarrotOffheapDataSegmentSize(DataSize size)
    {
        this.carrotOffheapDataSegmentSize = size;
        return this;
    }
    
    /**
     * Get carrot file cache segment size
     */
    public DataSize getCarrotFileDataSegmentSize()
    {
        return this.carrotFileDataSegmentSize;
    }
    
    @Config("orc.carrot-file-data-segment-size")
    @ConfigDescription("The data segment size for file cache")
    public OrcCacheConfig setCarrotFileDataSegmentSize(DataSize size)
    {
        this.carrotFileDataSegmentSize = size;
        return this;
    }
    
    @MinDataSize("0B")
    public DataSize getCarrotOffheapMaxCacheSize()
    {
        return this.carrotOffheapMaxCacheSize;
    }

    @Config("orc.carrot-offheap-cache-size")
    @ConfigDescription("Size of the parquet metadata carrot offheap cache")
    public OrcCacheConfig setCarrotOffheapMaxCacheSize(DataSize offheapCacheSize)
    {
      this.carrotOffheapMaxCacheSize = offheapCacheSize;
        return this;
    }
    @MinDataSize("0B")
    public DataSize getCarrotFileMaxCacheSize()
    {
        return this.carrotFileMaxCacheSize;
    }

    @Config("orc.carrot-file-cache-size")
    @ConfigDescription("Size of the parquet metadata carror file cache")
    public OrcCacheConfig setCarrotFileMaxCacheSize(DataSize fileCacheSize)
    {
      this.carrotFileMaxCacheSize = fileCacheSize;
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
    @Config("orc.carrot-cache-root-dir")
    public OrcCacheConfig setCarrotCacheRootDir(URI dir) {
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
    @Config("orc.carrot.jmx-metrics-enabled")
    public OrcCacheConfig setCarrotJMXMetricsEnabled(boolean b) {
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
    @Config("orc.carrot-cache-save-on-shutdown")
    public OrcCacheConfig setCarrotCacheSaveOnShutdown(boolean b) {
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
    @Config("orc.carrot-jmx-domain-name")
    public OrcCacheConfig setCarrotJMXDomainName(String domainName) {
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
    @Config("orc.carrot-admission-controller-enabled")
    public OrcCacheConfig setCarrotAdmissionControllerEnabled(boolean b) {
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
    @Config("orc.carrot-admission-controller-ratio")
    public OrcCacheConfig setCarrotAdmissionControllerRatio(double r) {
      this.carrotAdmissionControllerRatio = r;
      return this;
    }
    
    /** End of Carrot configuration section */
    
    public boolean isFileTailCacheEnabled()
    {
        return fileTailCacheEnabled;
    }

    @Config("orc.file-tail-cache-enabled")
    @ConfigDescription("Enable cache for orc file tail")
    public OrcCacheConfig setFileTailCacheEnabled(boolean fileTailCacheEnabled)
    {
        this.fileTailCacheEnabled = fileTailCacheEnabled;
        return this;
    }

    @MinDataSize("0B")
    public DataSize getFileTailCacheSize()
    {
        return fileTailCacheSize;
    }

    @Config("orc.file-tail-cache-size")
    @ConfigDescription("Size of the orc file tail cache")
    public OrcCacheConfig setFileTailCacheSize(DataSize fileTailCacheSize)
    {
        this.fileTailCacheSize = fileTailCacheSize;
        return this;
    }

    @MinDuration("0s")
    public Duration getFileTailCacheTtlSinceLastAccess()
    {
        return fileTailCacheTtlSinceLastAccess;
    }

    @Config("orc.file-tail-cache-ttl-since-last-access")
    @ConfigDescription("Time-to-live for orc file tail cache entry after last access")
    public OrcCacheConfig setFileTailCacheTtlSinceLastAccess(Duration fileTailCacheTtlSinceLastAccess)
    {
        this.fileTailCacheTtlSinceLastAccess = fileTailCacheTtlSinceLastAccess;
        return this;
    }

    public boolean isStripeMetadataCacheEnabled()
    {
        return stripeMetadataCacheEnabled;
    }

    @Config("orc.stripe-metadata-cache-enabled")
    @ConfigDescription("Enable cache for stripe metadata")
    public OrcCacheConfig setStripeMetadataCacheEnabled(boolean stripeMetadataCacheEnabled)
    {
        this.stripeMetadataCacheEnabled = stripeMetadataCacheEnabled;
        return this;
    }

    @MinDataSize("0B")
    public DataSize getStripeFooterCacheSize()
    {
        return stripeFooterCacheSize;
    }

    @Config("orc.stripe-footer-cache-size")
    @ConfigDescription("Size of the stripe footer cache")
    public OrcCacheConfig setStripeFooterCacheSize(DataSize stripeFooterCacheSize)
    {
        this.stripeFooterCacheSize = stripeFooterCacheSize;
        return this;
    }

    @MinDuration("0s")
    public Duration getStripeFooterCacheTtlSinceLastAccess()
    {
        return stripeFooterCacheTtlSinceLastAccess;
    }

    @Config("orc.stripe-footer-cache-ttl-since-last-access")
    @ConfigDescription("Time-to-live for stripe footer cache entry after last access")
    public OrcCacheConfig setStripeFooterCacheTtlSinceLastAccess(Duration stripeFooterCacheTtlSinceLastAccess)
    {
        this.stripeFooterCacheTtlSinceLastAccess = stripeFooterCacheTtlSinceLastAccess;
        return this;
    }

    @MinDataSize("0B")
    public DataSize getStripeStreamCacheSize()
    {
        return stripeStreamCacheSize;
    }

    @Config("orc.stripe-stream-cache-size")
    @ConfigDescription("Size of the stripe stream cache")
    public OrcCacheConfig setStripeStreamCacheSize(DataSize stripeStreamCacheSize)
    {
        this.stripeStreamCacheSize = stripeStreamCacheSize;
        return this;
    }

    @MinDuration("0s")
    public Duration getStripeStreamCacheTtlSinceLastAccess()
    {
        return stripeStreamCacheTtlSinceLastAccess;
    }

    @Config("orc.stripe-stream-cache-ttl-since-last-access")
    @ConfigDescription("Time-to-live for stripe stream cache entry after last access")
    public OrcCacheConfig setStripeStreamCacheTtlSinceLastAccess(Duration stripeStreamCacheTtlSinceLastAccess)
    {
        this.stripeStreamCacheTtlSinceLastAccess = stripeStreamCacheTtlSinceLastAccess;
        return this;
    }

    public boolean isRowGroupIndexCacheEnabled()
    {
        return rowGroupIndexCacheEnabled;
    }

    @Config("orc.row-group-index-cache-enabled")
    public OrcCacheConfig setRowGroupIndexCacheEnabled(boolean rowGroupIndexCacheEnabled)
    {
        this.rowGroupIndexCacheEnabled = rowGroupIndexCacheEnabled;
        return this;
    }

    @MinDataSize("0B")
    public DataSize getRowGroupIndexCacheSize()
    {
        return rowGroupIndexCacheSize;
    }

    @Config("orc.row-group-index-cache-size")
    @ConfigDescription("Size of the stripe row group index stream cache")
    public OrcCacheConfig setRowGroupIndexCacheSize(DataSize rowGroupIndexCacheSize)
    {
        this.rowGroupIndexCacheSize = rowGroupIndexCacheSize;
        return this;
    }

    @MinDuration("0s")
    public Duration getRowGroupIndexCacheTtlSinceLastAccess()
    {
        return rowGroupIndexCacheTtlSinceLastAccess;
    }

    @Config("orc.row-group-index-cache-ttl-since-last-access")
    @ConfigDescription("Time-to-live for stripe stream row group index cache entry after last access")
    public OrcCacheConfig setRowGroupIndexCacheTtlSinceLastAccess(Duration rowGroupIndexCacheTtlSinceLastAccess)
    {
        this.rowGroupIndexCacheTtlSinceLastAccess = rowGroupIndexCacheTtlSinceLastAccess;
        return this;
    }

    public boolean isDwrfStripeCacheEnabled()
    {
        return dwrfStripeCacheEnabled;
    }

    // DWRF format duplicates the stripe footer and index streams, and stores them before the footer.
    // DWRF StripeCache will reduce small IO's used to read the stripe footer and index streams.
    // Note when enabling dwrf stripe cache, increase the tail size so that footer and stripe cache
    // can be read in the same IO.
    @Config("orc.dwrf-stripe-cache-enabled")
    @ConfigDescription("Check DWRF stripe cache to look for stripe footers and index streams")
    public OrcCacheConfig setDwrfStripeCacheEnabled(boolean dwrfStripeCacheEnabled)
    {
        this.dwrfStripeCacheEnabled = dwrfStripeCacheEnabled;
        return this;
    }

    @MinDataSize("256B")
    public DataSize getExpectedFileTailSize()
    {
        return expectedFileTailSize;
    }

    @Config("orc.expected-file-tail-size")
    @ConfigDescription("Expected size of the file tail. This value should be increased to read StripeCache and footer in one IO")
    public OrcCacheConfig setExpectedFileTailSize(DataSize expectedFileTailSize)
    {
        this.expectedFileTailSize = expectedFileTailSize;
        return this;
    }
}
