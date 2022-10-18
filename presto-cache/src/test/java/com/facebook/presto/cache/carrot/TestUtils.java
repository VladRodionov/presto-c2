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

import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.RandomAccessFile;
import java.nio.file.FileVisitResult;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.nio.file.SimpleFileVisitor;
import java.nio.file.attribute.BasicFileAttributes;
import java.util.Iterator;
import java.util.Map;
import java.util.Random;

import org.apache.hadoop.conf.Configuration;

import com.carrot.cache.Cache;
import com.carrot.cache.Scavenger;
import com.carrot.cache.Scavenger.Stats;
import com.carrot.cache.util.CarrotConfig;
import com.facebook.airlift.log.Logger;
import com.facebook.presto.cache.CacheConfig;

public class TestUtils {
  private static final Logger LOG = Logger.get(TestUtils.class);

  public static File createTempFile() throws IOException {
    File f = File.createTempFile("carrot-temp", null);
    return f;
  }

  public static void fillRandom(File f, long size) throws IOException {
    Random r = new Random();
    FileOutputStream fos = new FileOutputStream(f);
    long written = 0;
    byte[] buffer = new byte[1024 * 1024];
    while (written < size) {
      r.nextBytes(buffer);
      int toWrite = (int) Math.min(buffer.length, size - written);
      fos.write(buffer, 0, toWrite);
      written += toWrite;
    }
    fos.close();
  }

  public static void touchFileAt(File f, long offset) throws IOException {
    Random r = new Random();
    long value = r.nextLong();
    RandomAccessFile raf = new RandomAccessFile(f, "rw");
    raf.seek(offset);
    raf.writeLong(value);
    raf.close();
  }
  /**
   * Deletes a file or a directory, recursively if it is a directory.
   *
   * If the path does not exist, nothing happens.
   *
   * @param path pathname to be deleted
   */
  public static void deletePathRecursively(String path) throws IOException {
    Path root = Paths.get(path);

    if (!Files.exists(root)) {
      return;
    }
    
    Files.walkFileTree(root, new SimpleFileVisitor<Path>() {
      @Override
      public FileVisitResult visitFile(Path file, BasicFileAttributes attrs) throws IOException {
        Files.delete(file);
        return FileVisitResult.CONTINUE;
      }

      @Override
      public FileVisitResult postVisitDirectory(Path dir, IOException e) throws IOException {
        if (e == null) {
          Files.delete(dir);
          return FileVisitResult.CONTINUE;
        } else {
          throw e;
        }
      }
    });
  }
  
  public static Configuration getHdfsConfiguration(CacheConfig cacheConfig, CarrotCacheConfig carrotCacheConfig)
  {
      CarrotCachingConfigurationProvider provider = new CarrotCachingConfigurationProvider(cacheConfig, carrotCacheConfig);
      Configuration configuration = new Configuration();
      provider.updateConfiguration(configuration, null /* ignored */, null /* ignored */);
      if (cacheConfig.getBaseDirectory() != null) {
        configuration.set(CarrotConfig.CACHE_ROOT_DIR_PATH_KEY,
          cacheConfig.getBaseDirectory().getPath());
      }
      return configuration;
  }
  
  public static Cache createCacheFromHdfsConfiguration(Configuration configuration) throws IOException {
    CarrotConfig config = CarrotConfig.getInstance(); 
    Iterator<Map.Entry<String, String>> it = configuration.iterator();
    while(it.hasNext()) {
      Map.Entry<String, String> entry = it.next();
      String name = entry.getKey();
      if (CarrotConfig.isCarrotPropertyName(name)) {
        config.setProperty(name, entry.getValue());
      }
    }
    config.setStartIndexNumberOfSlotsPower(CarrotCacheConfig.CACHE_NAME, 4);
    return new Cache(CarrotCacheConfig.CACHE_NAME, config);
  }
  
  public static void printStats(Cache cache) {
    LOG.info("Cache[%s]: storage size=%d data size=%d items=%d hit rate=%f, puts=%d, bytes written=%d\n",
             cache.getName(), cache.getStorageAllocated(), cache.getStorageUsed(), cache.size(),
             cache.getHitRate(), cache.getTotalWrites(), cache.getTotalWritesSize());
    Stats stats = Scavenger.getStatisticsForCache(cache.getName());
    LOG.info("Scavenger [%s]: runs=%d scanned=%d freed=%d written back=%d empty segments=%d\n", 
      cache.getName(), stats.getTotalRuns(), stats.getTotalBytesScanned(), 
      stats.getTotalBytesFreed(), stats.getTotalBytesScanned() - stats.getTotalBytesFreed(),
      stats.getTotalEmptySegments());
    
    cache = cache.getVictimCache();
    if (cache != null) {
      LOG.info("Cache[%s]: storage size=%d data size=%d items=%d hit rate=%f, puts=%d, bytes written=%d\n",
        cache.getName(), cache.getStorageAllocated(), cache.getStorageUsed(), cache.size(),
        cache.getHitRate(), cache.getTotalWrites(), cache.getTotalWritesSize());
      stats = Scavenger.getStatisticsForCache(cache.getName());
      LOG.info("Scavenger [%s]: runs=%d scanned=%d freed=%d written back=%d empty segments=%d\n", 
        cache.getName(), stats.getTotalRuns(), stats.getTotalBytesScanned(), 
        stats.getTotalBytesFreed(), stats.getTotalBytesScanned() - stats.getTotalBytesFreed(),
        stats.getTotalEmptySegments());
    }
  }
}
