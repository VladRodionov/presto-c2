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
package com.facebook.presto.operator;
import static com.facebook.presto.spi.page.PagesSerdeUtil.readPages;
import static com.facebook.presto.spi.page.PagesSerdeUtil.writePages;
import static com.google.common.hash.Hashing.md5;
import static com.google.common.util.concurrent.Futures.immediateFuture;
import static java.util.Objects.requireNonNull;

import java.io.Closeable;
import java.io.IOException;
import java.io.InputStream;
import java.io.UncheckedIOException;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.Iterator;
import java.util.List;
import java.util.Optional;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Future;

import javax.inject.Inject;

import org.weakref.jmx.Managed;

import com.carrot.cache.Builder;
import com.carrot.cache.Cache;
import com.carrot.cache.controllers.AQBasedAdmissionController;
import com.carrot.cache.controllers.MinAliveRecyclingSelector;
import com.carrot.cache.index.CompactBaseWithExpireIndexFormat;
import com.carrot.cache.io.BaseDataWriter;
import com.carrot.cache.io.BaseFileDataReader;
import com.carrot.cache.io.BaseMemoryDataReader;
import com.carrot.cache.util.Utils;
import com.facebook.airlift.log.Logger;
import com.facebook.presto.common.Page;
import com.facebook.presto.common.block.BlockEncodingSerde;
import com.facebook.presto.execution.buffer.PagesSerdeFactory;
import com.facebook.presto.metadata.Split;
import com.facebook.presto.metadata.Split.SplitIdentifier;
import com.google.common.annotations.VisibleForTesting;
import com.google.common.collect.AbstractIterator;

import io.airlift.slice.InputStreamSliceInput;
import io.airlift.slice.OutputStreamSliceOutput;
import io.airlift.slice.SliceOutput;
import io.airlift.units.DataSize;
import io.airlift.units.DataSize.Unit;

/**
 * Initially supports only hive-connector
 * Plans to add more: delta, hudi, iceberg 
 */
public class CarrotFragmentResultCacheManager
        implements FragmentResultCacheManager
{
  
    public static String CACHE_TYPE_NAME = "CARROT";
    
    private static final String CACHE_NAME = "fragment-result";
    
    private static final Logger log = Logger.get(CarrotFragmentResultCacheManager.class);

    private final Path baseDirectory;
    // Max size for the in-memory buffer.
    private final long maxInFlightBytes;
    // Size limit for every single page.
    private final long maxSinglePagesBytes;
    // Max on-disk size for this fragment result cache.
    private final long maxCacheBytes;
    private final PagesSerdeFactory pagesSerdeFactory;
    private final FragmentCacheStats fragmentCacheStats;
    private final ExecutorService flushExecutor;
    
    private final DataSize segmentSize = new DataSize(128, Unit.MEGABYTE);

    private long cacheTtl;
    private boolean acEnabled;
    private Cache cache;

    @Inject
    public CarrotFragmentResultCacheManager(
            FileFragmentResultCacheConfig cacheConfig,
            BlockEncodingSerde blockEncodingSerde,
            FragmentCacheStats fragmentCacheStats,
            ExecutorService flushExecutor)
    {
        requireNonNull(cacheConfig, "cacheConfig is null");
        requireNonNull(blockEncodingSerde, "blockEncodingSerde is null");
        this.baseDirectory = Paths.get(cacheConfig.getBaseDirectory());
        this.maxInFlightBytes = cacheConfig.getMaxInFlightSize().toBytes();
        this.maxSinglePagesBytes = cacheConfig.getMaxSinglePagesSize().toBytes();
        this.maxCacheBytes = cacheConfig.getMaxCacheSize().toBytes();
        // pagesSerde is not thread safe
        this.pagesSerdeFactory = new PagesSerdeFactory(blockEncodingSerde, cacheConfig.isBlockEncodingCompressionEnabled());
        this.fragmentCacheStats = requireNonNull(fragmentCacheStats, "fragmentCacheStats is null");
        this.flushExecutor = requireNonNull(flushExecutor, "flushExecutor is null");
        this.cacheTtl = cacheConfig.getCacheTtl().toMillis();
        acEnabled = cacheConfig.isCarrotAdmissionControllerEnabled();
        boolean jmxEnabled = cacheConfig.isCarrotJmxEnabled();
        try {
          this.cache = getCache();
        } catch (IOException e) {
          log.error("Creating cache[%s] FAILED", CACHE_NAME);
          log.error(e);
          return;
        }

        if (jmxEnabled) {
          cache.registerJMXMetricsSink(cacheConfig.getJmxDomainName());
        }
    }
    
    public void registerShutDownHook() {
      if (cache != null) {
        // Add shutdown hook
        Runtime.getRuntime().addShutdownHook( new Thread(() -> {
          log.info("Shutting down cache[%s]", cache.getName());
          try {
            this.cache.shutdown();
            log.info("Shutting down cache[%s] DONE", cache.getName());

          } catch (IOException e) {
            log.error("Shutting down cache[%s] FAILED", cache.getName());
            log.error(e);
          }
        }));
        log.error("CFRCM shutdown hook registered");
      }
    }
    
    private Cache getCache() throws IOException {
      // Try load first
      Cache cache = null;
      try {
        cache = Cache.loadCache(baseDirectory.toString(), CACHE_NAME);
      } catch (IOException e) {
        log.error(e);
      }
      if (cache == null) {
        // Build new one
        Builder builder = new Builder(CACHE_NAME);
        
        builder
          .withCacheDataSegmentSize(segmentSize.toBytes())
          .withCacheMaximumSize(maxCacheBytes)
          .withRecyclingSelector(MinAliveRecyclingSelector.class.getName())
          .withDataWriter(BaseDataWriter.class.getName())
          .withMemoryDataReader(BaseMemoryDataReader.class.getName())
          .withFileDataReader(BaseFileDataReader.class.getName())
          .withMainQueueIndexFormat(CompactBaseWithExpireIndexFormat.class.getName())
          .withCacheRootDir(baseDirectory.toString())
          .withCacheStreamingSupportBufferSize(1 << 22); // 4MB
          // Do we need admission controller?
          if (acEnabled) {
            builder.withAdmissionController(AQBasedAdmissionController.class.getName());
          }
          return builder.buildDiskCache();
      }
      return cache;
    }
    
    private boolean isCompatibleSplit(Split split) {
      // Verify this
      return split.getConnectorSplit().getClass().getName().endsWith("HiveSplit") ||
             split.getConnectorId().getCatalogName().equals("test"); // to support testing
    }
    
    private boolean exists(byte[] rawKey) {
      // Create first key with offset=0 (last 8 bytes)
      byte[] key = new byte[rawKey.length + Utils.SIZEOF_LONG];
      System.arraycopy(rawKey, 0, key, 0, rawKey.length);
      // Bit hack, we use index format, which supports item size
      return this.cache.getEngine().getMemoryIndex().getItemSize(key, 0, key.length) > 0;
    }
    
    @SuppressWarnings("deprecation")
    private byte[] getRawKey(String serializedPlan, SplitIdentifier id) {
      return md5().hashBytes((serializedPlan + id.toString()).getBytes()).asBytes();
    }
    
    @Override
    public Future<?> put(String serializedPlan, Split split, List<Page> result)
    {
      // We support only 
        if (!isCompatibleSplit(split)) {
          return immediateFuture(null);
        }
        
        byte[] rawKey = getRawKey(serializedPlan, split.getSplitIdentifier());
        long resultSize = getPagesSize(result);
        if (fragmentCacheStats.getInFlightBytes() + resultSize > maxInFlightBytes ||
                exists(rawKey) ||
                resultSize > maxSinglePagesBytes ||
                // Here we use the logical size resultSize as an estimate for admission control.
                fragmentCacheStats.getCacheSizeInBytes() + resultSize > maxCacheBytes) {
            return immediateFuture(null);
        }

        fragmentCacheStats.addInFlightBytes(resultSize);
        return flushExecutor.submit(() -> cachePages(rawKey, result, resultSize));
    }

    private static long getPagesSize(List<Page> pages)
    {
        return pages.stream()
                .mapToLong(Page::getSizeInBytes)
                .sum();
    }

    private void cachePages(byte[] key,  List<Page> pages, long resultSize)
    {
        try {
            try (SliceOutput output = new OutputStreamSliceOutput(cache.getOutputStream(key, 0, key.length, 
              this.cacheTtl + System.currentTimeMillis()))) {
                writePages(pagesSerdeFactory.createPagesSerde(), output, pages.iterator());
                long resultPhysicalBytes = output.size();
                if (resultPhysicalBytes > 0) {
                  fragmentCacheStats.incrementCacheEntries();
                  fragmentCacheStats.addCacheSizeInBytes(resultPhysicalBytes);
                }
            }
            catch (UncheckedIOException | IOException e) {
                log.warn(e, "%s encountered an error while writing to cache", Thread.currentThread().getName());
            }
        }
        catch (UncheckedIOException e) {
            log.warn(e, "%s encountered an error while writing to cache", Thread.currentThread().getName());
        }
        finally {
            fragmentCacheStats.addInFlightBytes(-resultSize);
        }
    }


    @Override
    public Optional<Iterator<Page>> get(String serializedPlan, Split split)
    {
  
      byte[] rawKey = getRawKey(serializedPlan, split.getSplitIdentifier());
        
        try {
            InputStream inputStream = this.cache.getInputStream(rawKey, 0, rawKey.length);
            if (inputStream == null) {
              fragmentCacheStats.incrementCacheMiss();
              return Optional.empty();
            }
            Iterator<Page> result = readPages(pagesSerdeFactory.createPagesSerde(), new InputStreamSliceInput(inputStream));
            fragmentCacheStats.incrementCacheHit();
            return Optional.of(closeWhenExhausted(result, inputStream));
        }
        catch (UncheckedIOException | IOException e) {
            log.error(e, "read fragment %s error", split.getSplitIdentifier().toString());
            // there might be a chance the file has been deleted. We would return cache miss in this case.
            fragmentCacheStats.incrementCacheMiss();
            return Optional.empty();
        }
    }

    @Managed
    public void invalidateAllCache()
    {
      //TODO
    }

    private static <T> Iterator<T> closeWhenExhausted(Iterator<T> iterator, Closeable resource)
    {
        requireNonNull(iterator, "iterator is null");
        requireNonNull(resource, "resource is null");

        return new AbstractIterator<T>()
        {
            @Override
            protected T computeNext()
            {
                if (iterator.hasNext()) {
                    return iterator.next();
                }
                try {
                    resource.close();
                }
                catch (IOException e) {
                    throw new UncheckedIOException(e);
                }
                return endOfData();
            }
        };
    }
    
    @VisibleForTesting
    void dispose() {
      this.cache.dispose();
    }
}
