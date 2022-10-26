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
import static org.testng.Assert.assertTrue;

import java.io.File;
import java.io.IOException;
import java.lang.management.ManagementFactory;
import java.net.URI;
import java.nio.ByteBuffer;
import java.util.Random;

import javax.management.MBeanServer;
import javax.management.ObjectName;

import org.apache.commons.math3.distribution.ZipfDistribution;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.Path;
import org.testng.annotations.AfterClass;
import org.testng.annotations.AfterMethod;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

import com.carrot.cache.Cache;
import com.carrot.cache.util.Epoch;
import com.carrot.cache.util.Utils;
import com.facebook.airlift.log.Logger;
import com.facebook.presto.cache.CacheConfig;

import io.airlift.units.DataSize;
import io.airlift.units.DataSize.Unit;

@Test(singleThreaded = true)
public class TestCarrotCachingInputStream {
  
  protected boolean fillFile = false;
  
  private static final Logger LOG = Logger.get(TestCarrotCachingInputStream.class);

  private URI cacheDirectory;
  
  private File sourceFile;
  
  private DataSize cacheSize = new DataSize(4, Unit.GIGABYTE);
  
  private DataSize cacheSegmentSize = new DataSize(20, Unit.MEGABYTE);
  
  private DataSize fileSize = new DataSize(10, Unit.GIGABYTE);

  private double zipfAlpha = 0.9;
  
  private Cache cache;
  
  int pageSize;
  
  int ioBufferSize;
  
  String domainName;
  
  boolean skipTest = false;
    
  @BeforeClass
  public void setupClass() throws IOException {
    int javaVersion = Utils.getJavaVersion();
    if (javaVersion < 11) {
      skipTest = true;
      LOG.warn("Skipping " + getClass().getName() + " java version 11 and above is required");
      return;
    }
    this.sourceFile = TestUtils.createTempFile();
    if (fillFile) {
      TestUtils.fillRandom(sourceFile, fileSize.toBytes());
    }
  }
  
  @AfterClass
  public void tearDown() {
      if (skipTest) {
        return;
      }
      sourceFile.delete();
      LOG.info("Deleted %s", sourceFile.getAbsolutePath());
  }
  
  @BeforeMethod
  public void setup()
          throws IOException
  {
    if (skipTest) return;
    LOG.info("%s BeforeMethod", Thread.currentThread().getName());  
    this.cacheDirectory = createTempDirectory("carrot_cache").toUri();
      Epoch.reset();
  }
  
  @AfterMethod
  public void close() throws IOException {
    if (skipTest) return;
    unregisterJMXMetricsSink(cache);
    cache.dispose();
    checkState(cacheDirectory != null);
    TestUtils.deletePathRecursively(cacheDirectory.getPath());
    LOG.info("Deleted %s", cacheDirectory);
  }
  
  private Cache createCache(boolean acEnabled) throws IOException {
    CacheConfig cacheConfig = new CacheConfig()
        .setCacheType(CARROT)
        .setCachingEnabled(true)
        .setBaseDirectory(cacheDirectory);
    CarrotCacheConfig carrotCacheConfig = new CarrotCacheConfig()
        .setMaxCacheSize(cacheSize)
        .setDataSegmentSize(cacheSegmentSize)
        .setCacheAdmissionControllerEnabled(acEnabled)
        .setCacheAdmissionQueueSizeRatio(0.2)
        .setSLRUEvictionInsertionPoint(6)
        .setStartIndexSlotsPower(4)
        .setRecyclingSelector(RecyclingSelector.MIN_ALIVE)
        .setMetricsCollectionEnabled(true);
    
    Configuration configuration = TestUtils.getHdfsConfiguration(cacheConfig, carrotCacheConfig);
    cache = TestUtils.createCacheFromHdfsConfiguration(configuration);
    LOG.info("Recycling selector=%s\n", cache.getEngine().getRecyclingSelector().getClass().getName());
    this.pageSize = configuration.getInt("cache.carrot.data-page-size", 0);
    this.ioBufferSize = configuration.getInt("cache.carrot.io-buffer-size", 0);
    boolean metricsEnabled = carrotCacheConfig.isMetricsCollectionEnabled();
    if (metricsEnabled) {
      domainName = carrotCacheConfig.getMetricsDomain();
      this.cache.registerJMXMetricsSink(domainName);
    }
    return cache;
  }
  
  private void unregisterJMXMetricsSink(Cache cache) {
    if (domainName == null) {
      return;
    }
    MBeanServer mbs = ManagementFactory.getPlatformMBeanServer(); 
    ObjectName name;
    try {
      name = new ObjectName(String.format("%s:type=cache,name=%s",domainName, cache.getName()));
      mbs.unregisterMBean(name); 
    } catch (Exception e) {
      LOG.error(e);
    }
    Cache victimCache = cache.getVictimCache();
    if (victimCache != null) {
      unregisterJMXMetricsSink(victimCache);
    }
  }
  
  @Test
  public void testCarrotCachingInputStreamACDisabled () throws IOException {
    if (skipTest) return;

    System.out.printf("Java version=%d\n", Utils.getJavaVersion());
    this.cache = createCache(false);
    runTestRandomAccess();
  }
  
  @Test
  public void testCarrotCachingInputStreamACEnabledSeq () throws IOException {
    if (skipTest) return;

    this.cache = createCache(true);
    runTestRandomSequentialAccess();
  }
  
  @Test
  public void testCarrotCachingInputStreamACEnabled () throws IOException {
    if (skipTest) return;

    System.out.printf("Java version=%d\n", Utils.getJavaVersion());
    this.cache = createCache(true);
    Runnable r = () -> {
      try {
        runTestRandomAccess();
      } catch (IOException e) {
        LOG.error(e);
      }
    };
    
    Thread[] runners = new Thread[4];
    for(int i = 0; i < runners.length; i++) {
      runners[i] = new Thread(r);
      runners[i].start();
    }
    for(int i = 0; i < runners.length; i++) {
      try {
        runners[i].join();
      } catch (InterruptedException e) {
      }
    }
  }

  @Test
  public void testCarrotCachingInputStreamACDisabledSeq () throws IOException {
    if (skipTest) return;

    this.cache = createCache(false);
    runTestRandomSequentialAccess();
  }
  
  @Test
  public void testCarrotCachingInputStreamNotPositionalReads() throws IOException {
    if (skipTest) return;

    this.cache = createCache(false);
    runTestSequentialAccess();
  }
  
  @Test
  public void testCarrotCachingInputStreamHeapByteBuffer() throws IOException {
    if (skipTest) return;

    this.cache = createCache(false);
    runTestSequentialAccessByteBuffer(false);
  }
  
  @Test
  public void testCarrotCachingInputStreamDirectByteBuffer() throws IOException {
    if (skipTest) return;

    this.cache = createCache(false);
    runTestSequentialAccessByteBuffer(true);
  }
  
  protected FSDataInputStream getExternalStream() throws IOException {
    return new FSDataInputStream(new VirtualFileInputStream(fileSize.toBytes()));
  }
  
  private void runTestRandomAccess() throws IOException {
    
    FSDataInputStream extStream = getExternalStream();
    long fileLength = fileSize.toBytes();
    try (
        CarrotCachingInputStream carrotStream = new CarrotCachingInputStream(cache,
            new Path(this.sourceFile.toURI()), extStream, fileLength, pageSize, ioBufferSize);) 
    {
      FSDataInputStream cacheStream = new FSDataInputStream(carrotStream);
      int numRecords = (int) (fileSize.toBytes() / pageSize);
      ZipfDistribution dist = new ZipfDistribution(numRecords, this.zipfAlpha);

      int numIterations = numRecords * 10;

      Random r = new Random();
      byte[] buffer = new byte[pageSize];
      byte[] controlBuffer = new byte[pageSize];
      long startTime = System.currentTimeMillis();
      long totalRead = 0;
      
      for (int i = 0; i < numIterations; i++) {
        long n = dist.sample();
        long offset = (n - 1) * pageSize;

        int requestOffset = r.nextInt(pageSize / 2);
        // This can cross
        int requestSize = r.nextInt(pageSize/* - requestOffset */);
        offset += requestOffset;
        requestSize = (int) Math.min(requestSize, fileLength - offset);

        long t1 = System.nanoTime();
        extStream.readFully(offset, controlBuffer, 0, requestSize);
        long t2 = System.nanoTime();
        cacheStream.readFully(offset, buffer, 0, requestSize);
        long t3 = System.nanoTime();
        //
        totalRead += requestSize;
        boolean result = Utils.compareTo(buffer, 0, requestSize, controlBuffer, 0, requestSize) == 0;
        if (!result) {
          LOG.error("i=%d file length=%d offset=%d requestSize=%d", i, fileSize.toBytes(), offset, requestSize);
        }
        assertTrue(result);
        if (i > 0 && i % 10000 == 0) {
          LOG.info("%s: read %d offset=%d size=%d direct read=%d cache read=%d", 
            Thread.currentThread().getName(), i, offset, requestSize,
            (t2 - t1) / 1000, (t3 - t2) / 1000);
        }
      }
      long endTime = System.currentTimeMillis();
      LOG.info("Test finished in %dms total read=%d", (endTime - startTime), totalRead);
      TestUtils.printStats(cache);
    }
  }
  
  private void runTestRandomSequentialAccess() throws IOException {
    FSDataInputStream extStream = getExternalStream();
    long fileLength = fileSize.toBytes();

    try (CarrotCachingInputStream carrotStream = new CarrotCachingInputStream(cache,
        new Path(this.sourceFile.toURI()), extStream, fileLength, pageSize, ioBufferSize);) {

      FSDataInputStream cacheStream = new FSDataInputStream(carrotStream);
      int numRecords = (int) (fileSize.toBytes() / pageSize);
      ZipfDistribution dist = new ZipfDistribution(numRecords, this.zipfAlpha);

      int numIterations = numRecords * 10;

      Random r = new Random();
      byte[] buffer = new byte[pageSize];
      byte[] controlBuffer = new byte[pageSize];
      long startTime = System.currentTimeMillis();
      long totalRead = 0;
      for (int i = 0; i < numIterations; i++) {
        int accessSize = 8 * 1024;
        long n = dist.sample();
        long offset = (n - 1) * pageSize;
        int requestOffset = r.nextInt(pageSize);
        int requestSize = this.ioBufferSize;
        offset += requestOffset;
        requestSize = (int) Math.min(requestSize, fileLength - offset);
        long t1 = System.nanoTime();
        int m = requestSize / accessSize;
        if (m == 0) {
          accessSize = requestSize;
        }
        for (int k = 0; k < m; k++) {

          extStream.readFully(offset, controlBuffer, 0, accessSize);
          cacheStream.readFully(offset, buffer, 0, accessSize);
          assertTrue(Utils.compareTo(buffer, 0, accessSize, controlBuffer, 0, accessSize) == 0);
          offset += accessSize;
          totalRead += accessSize;
        }
        long t2 = System.nanoTime();

        if (i > 0 && i % 10000 == 0) {
          LOG.info("read %d offset=%d size=%d  read time=%d", i, offset, requestSize,
            (t2 - t1) / 1000);
        }
      }
      long endTime = System.currentTimeMillis();
      LOG.info("Test finished in %dms total read=%d", (endTime - startTime), totalRead);
      TestUtils.printStats(cache);
    }
  }
  
  private void runTestSequentialAccess() throws IOException {
    FSDataInputStream extStream = getExternalStream();
    long fileLength = fileSize.toBytes();

    try (CarrotCachingInputStream carrotStream = new CarrotCachingInputStream(cache,
        new Path(this.sourceFile.toURI()), extStream, fileLength, pageSize, ioBufferSize);) {

      FSDataInputStream cacheStream = new FSDataInputStream(carrotStream);
      int requestSize = 8 * 1024;
      int numRecords = 1000;
      byte[] buffer = new byte[pageSize];
      byte[] controlBuffer = new byte[pageSize];
      long startTime = System.currentTimeMillis();
      long totalRead = 0;
      for (int i = 0; i < numRecords; i++) {

        int readExt =  extStream.read(controlBuffer, 0, requestSize);
        int readCache = cacheStream.read(buffer, 0, requestSize);
        //LOG.info("ext pos=%d cache pos=%d\n", extStream.getPos(), cacheStream.getPos());
        assertEquals(readExt, requestSize);
        assertEquals(readCache, requestSize);
        assertTrue(Utils.compareTo(buffer, 0, requestSize, controlBuffer, 0, requestSize) == 0);
        assertEquals(extStream.getPos(), cacheStream.getPos());
        if ( i > 0 && i % 100000 == 0) {
          LOG.info("read %d", i);
        }
      }
      long endTime = System.currentTimeMillis();
      LOG.info("Test finished in %dms total read requests=%d", (endTime - startTime), totalRead);
      TestUtils.printStats(cache);
    }
  }
  
  private void runTestSequentialAccessByteBuffer(boolean direct) throws IOException {
    FSDataInputStream extStream = getExternalStream();
    long fileLength = fileSize.toBytes();

    try (CarrotCachingInputStream carrotStream = new CarrotCachingInputStream(cache,
        new Path(this.sourceFile.toURI()), extStream, fileLength, pageSize, ioBufferSize);) {

      FSDataInputStream cacheStream = new FSDataInputStream(carrotStream);
      int requestSize = 8 * 1024;
      int numRecords = 1000;
      ByteBuffer buffer = direct? ByteBuffer.allocateDirect(requestSize): ByteBuffer.allocate(requestSize);
      byte[] controlBuffer = new byte[requestSize];
      long startTime = System.currentTimeMillis();
      for (int i = 0; i < numRecords; i++) {

        int readExt =  extStream.read(controlBuffer);
        int readCache = cacheStream.read(buffer);
        assertEquals(readExt, requestSize);
        assertEquals(readCache, requestSize);
        assertEquals(extStream.getPos(), cacheStream.getPos());
        buffer.flip();
        assertTrue(Utils.compareTo(buffer, requestSize, controlBuffer, 0, requestSize) == 0);
        if ( i > 0 && i % 100000 == 0) {
          LOG.info("read %d", i);
        }
        buffer.clear();
      }
      long endTime = System.currentTimeMillis();
      LOG.info("Test finished in %dms total read requests=%d", (endTime - startTime), numRecords);
      TestUtils.printStats(cache);
    }
  }
}
