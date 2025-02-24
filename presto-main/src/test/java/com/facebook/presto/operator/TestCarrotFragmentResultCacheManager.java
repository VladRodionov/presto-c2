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

import com.facebook.presto.common.Page;
import com.facebook.presto.common.block.TestingBlockEncodingSerde;
import com.facebook.presto.metadata.Split;
import com.facebook.presto.spi.ConnectorId;
import com.facebook.presto.spi.ConnectorSplit;
import com.facebook.presto.spi.HostAddress;
import com.facebook.presto.spi.NodeProvider;
import com.facebook.presto.spi.connector.ConnectorTransactionHandle;
import com.facebook.presto.spi.schedule.NodeSelectionStrategy;
import com.google.common.collect.ImmutableList;
import io.airlift.units.DataSize;
import org.testng.annotations.AfterClass;
import org.testng.annotations.Test;

import java.io.File;
import java.io.IOException;
import java.net.URI;
import java.nio.file.Files;
import java.util.Iterator;
import java.util.List;
import java.util.Objects;
import java.util.Optional;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;

import static com.facebook.airlift.concurrent.Threads.daemonThreadsNamed;
import static com.facebook.presto.block.BlockAssertions.createStringsBlock;
import static com.facebook.presto.spi.schedule.NodeSelectionStrategy.NO_PREFERENCE;
import static com.google.common.base.MoreObjects.toStringHelper;
import static com.google.common.base.Preconditions.checkState;
import static java.nio.file.Files.createTempDirectory;
import static java.util.concurrent.Executors.newScheduledThreadPool;
import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertFalse;
import static org.testng.Assert.assertNull;
import static org.testng.Assert.assertTrue;

public class TestCarrotFragmentResultCacheManager
{
    private static final String SERIALIZED_PLAN_FRAGMENT_1 = "test plan fragment 1";
    private static final String SERIALIZED_PLAN_FRAGMENT_2 = "test plan fragment 2";
    private static final Split SPLIT_1 = new Split(new ConnectorId("test"), new ConnectorTransactionHandle() {}, new TestingSplit(1));
    private static final Split SPLIT_2 = new Split(new ConnectorId("test"), new ConnectorTransactionHandle() {}, new TestingSplit(2));

    private final ExecutorService writeExecutor = newScheduledThreadPool(5, daemonThreadsNamed("test-cache-flusher-%s"));
    private final ExecutorService multithreadingWriteExecutor = newScheduledThreadPool(10, daemonThreadsNamed("test-cache-multithreading-flusher-%s"));

    @AfterClass
    public void close()
            throws IOException, InterruptedException
    {
        writeExecutor.shutdown();
        multithreadingWriteExecutor.shutdown();
    }

    private URI getNewCacheDirectory(String prefix)
            throws Exception
    {
        return createTempDirectory(prefix).toUri();
    }

    private void cleanupCacheDirectory(URI cacheDirectory)
            throws IOException
    {
        checkState(cacheDirectory != null);
        File[] files = new File(cacheDirectory).listFiles();
        if (files != null) {
            for (File file : files) {
              if (file.isDirectory()) {
                deleteDirectory(file);
              } else {
                Files.deleteIfExists(file.toPath());
              }
            }
        }
        Files.deleteIfExists(new File(cacheDirectory).toPath());
    }
    
    private void deleteDirectory(File file) throws IOException {
      File[] files = file.listFiles();
      if (files != null) {
          for (File f : files) {
            if (f.isDirectory()) {
              deleteDirectory(f);
            } else {
              Files.deleteIfExists(f.toPath());
            }
          }
      }
      Files.deleteIfExists(file.toPath());
    }

    @Test
    public void testBasic()
            throws Exception
    {
        URI cacheDirectory = getNewCacheDirectory("testBasic");
        FragmentCacheStats stats = new FragmentCacheStats();
        CarrotFragmentResultCacheManager cacheManager = carrotFragmentResultCacheManager(stats, cacheDirectory);

        // Test fetching new fragment. Current cache status: empty
        assertFalse(cacheManager.get(SERIALIZED_PLAN_FRAGMENT_1, SPLIT_1).isPresent());
        assertEquals(stats.getCacheMiss(), 1);
        assertEquals(stats.getCacheHit(), 0);
        assertEquals(stats.getCacheEntries(), 0);
        assertEquals(stats.getCacheSizeInBytes(), 0);

        // Test empty page. Current cache status: empty
        cacheManager.put(SERIALIZED_PLAN_FRAGMENT_1, SPLIT_1, ImmutableList.of()).get();
        Optional<Iterator<Page>> result = cacheManager.get(SERIALIZED_PLAN_FRAGMENT_1, SPLIT_1);
        assertFalse(result.isPresent());
        assertEquals(stats.getCacheMiss(), 2);
        assertEquals(stats.getCacheHit(), 0);
        assertEquals(stats.getCacheEntries(), 0);
        assertEquals(stats.getCacheSizeInBytes(), 0);

        // Test non-empty page. Current cache status: { (plan1, split1) -> [] }
        List<Page> pages = ImmutableList.of(new Page(createStringsBlock("plan-1-split-2")));
        cacheManager.put(SERIALIZED_PLAN_FRAGMENT_2, SPLIT_2, pages).get();
        result = cacheManager.get(SERIALIZED_PLAN_FRAGMENT_2, SPLIT_2);
        assertTrue(result.isPresent());
        assertPagesEqual(result.get(), pages.iterator());
        assertEquals(stats.getCacheMiss(), 2);
        assertEquals(stats.getCacheHit(), 1);
        assertEquals(stats.getCacheEntries(), 1);

        // Test cache miss for plan mismatch and split mismatch. Current cache status: { (plan1, split1) -> [], (plan2, split2) -> ["plan-1-split-2"] }
        cacheManager.get(SERIALIZED_PLAN_FRAGMENT_1, SPLIT_2);
        assertEquals(stats.getCacheMiss(), 3);
        assertEquals(stats.getCacheHit(), 1);
        assertEquals(stats.getCacheEntries(), 1);
        cacheManager.get(SERIALIZED_PLAN_FRAGMENT_2, SPLIT_1);
        assertEquals(stats.getCacheMiss(), 4);
        assertEquals(stats.getCacheHit(), 1);
        assertEquals(stats.getCacheEntries(), 1);

        cacheManager.dispose();
        cleanupCacheDirectory(cacheDirectory);
    }

 
    private static void assertPagesEqual(Iterator<Page> pages1, Iterator<Page> pages2)
    {
        while (pages1.hasNext() && pages2.hasNext()) {
            Page page1 = pages1.next();
            Page page2 = pages2.next();
            assertEquals(page1.getChannelCount(), page2.getChannelCount());
            for (int i = 0; i < page1.getChannelCount(); i++) {
                assertTrue(page1.getBlock(i).equals(0, 0, page2.getBlock(i), 0, 0, page1.getBlock(0).getSliceLength(0)));
            }
        }
        assertFalse(pages1.hasNext());
        assertFalse(pages2.hasNext());
    }

    @Test
    public void testThreadWrite()
            throws Exception
    {
        URI cacheDirectory = getNewCacheDirectory("testThreadWrite");
        String writeThreadNameFormat = "test write content,thread %s,%s";
        FragmentCacheStats stats = new FragmentCacheStats();
        CarrotFragmentResultCacheManager threadWriteCacheManager = carrotFragmentResultCacheManager(stats, cacheDirectory);
        ImmutableList.Builder<Future<Boolean>> futures = ImmutableList.builder();
        for (int i = 0; i < 10; i++) {
            Future<Boolean> future = multithreadingWriteExecutor.submit(() -> {
                try {
                    String threadInfo = String.format(writeThreadNameFormat, Thread.currentThread().getName(), Thread.currentThread().getId());
                    List<Page> pages = ImmutableList.of(new Page(createStringsBlock(threadInfo)));
                    threadWriteCacheManager.put(threadInfo, SPLIT_2, pages).get();
                    Optional<Iterator<Page>> result = threadWriteCacheManager.get(threadInfo, SPLIT_2);
                    assertTrue(result.isPresent());
                    assertPagesEqual(result.get(), pages.iterator());
                    return true;
                }
                catch (Exception e) {
                    return false;
                }
            });
            futures.add(future);
        }
        for (Future<Boolean> future : futures.build()) {
            assertTrue(future.get(30, TimeUnit.SECONDS));
        }

        assertTrue(stats.getCacheSizeInBytes() > 0);

        threadWriteCacheManager.dispose();
        cleanupCacheDirectory(cacheDirectory);
    }

    private CarrotFragmentResultCacheManager carrotFragmentResultCacheManager(FragmentCacheStats fragmentCacheStats, URI cacheDirectory)
    {
      FileFragmentResultCacheConfig config = new FileFragmentResultCacheConfig();
      config.setCacheTypeName("CARROT");
      config.setCarrotAdmissionControllerEnabled(false);
      return carrotFragmentResultCacheManager(fragmentCacheStats, config, cacheDirectory);
    }

    private CarrotFragmentResultCacheManager carrotFragmentResultCacheManager(FragmentCacheStats fragmentCacheStats, FileFragmentResultCacheConfig cacheConfig, URI cacheDirectory)
    {
        return new CarrotFragmentResultCacheManager(
                cacheConfig.setBaseDirectory(cacheDirectory),
                new TestingBlockEncodingSerde(),
                fragmentCacheStats,
                writeExecutor);
    }

    private static class TestingSplit
            implements ConnectorSplit
    {
        private final int id;

        public TestingSplit(int id)
        {
            this.id = id;
        }

        @Override
        public NodeSelectionStrategy getNodeSelectionStrategy()
        {
            return NO_PREFERENCE;
        }

        @Override
        public List<HostAddress> getPreferredNodes(NodeProvider nodeProvider)
        {
            return ImmutableList.of();
        }

        @Override
        public Object getInfo()
        {
            return this;
        }

        @Override
        public Object getSplitIdentifier()
        {
            return id;
        }

        @Override
        public boolean equals(Object o)
        {
            if (this == o) {
                return true;
            }
            if (o == null || getClass() != o.getClass()) {
                return false;
            }
            TestingSplit that = (TestingSplit) o;
            return id == that.id;
        }

        @Override
        public int hashCode()
        {
            return Objects.hash(id);
        }

        @Override
        public String toString()
        {
            return toStringHelper(this)
                    .add("id", id)
                    .toString();
        }
    }
}
