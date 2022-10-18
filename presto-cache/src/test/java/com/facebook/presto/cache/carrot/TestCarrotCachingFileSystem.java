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
import static io.airlift.units.DataSize.Unit.KILOBYTE;
import static java.nio.file.Files.createTempDirectory;
import static org.testng.Assert.assertTrue;

import java.io.File;
import java.io.IOException;
import java.io.RandomAccessFile;
import java.net.URI;
import java.net.URISyntaxException;
import java.util.Optional;
import java.util.OptionalLong;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.permission.FsPermission;
import org.apache.hadoop.util.Progressable;
import org.testng.annotations.AfterClass;
import org.testng.annotations.AfterMethod;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

import com.facebook.airlift.log.Logger;
import com.facebook.presto.cache.CacheConfig;
import com.facebook.presto.hive.CacheQuota;
import com.facebook.presto.hive.HiveFileContext;
import com.facebook.presto.hive.filesystem.ExtendedFileSystem;

import io.airlift.units.DataSize;
import io.airlift.units.DataSize.Unit;

@Test(singleThreaded = true)
public class TestCarrotCachingFileSystem {
  
  private static final Logger LOG = Logger.get(TestCarrotCachingFileSystem.class);

  private URI cacheDirectory;
  
  private File sourceFile;
  
  private DataSize cacheSize = new DataSize(400, Unit.MEGABYTE);
  
  private DataSize cacheSegmentSize = new DataSize(20, Unit.MEGABYTE);
  
  private DataSize fileSize = new DataSize(1, Unit.GIGABYTE);
  
  int pageSize;
  
  int ioBufferSize;
  
  ExtendedFileSystem fs;
  
  @BeforeClass
  public void setupClass() throws IOException {
    this.sourceFile = TestUtils.createTempFile();
    TestUtils.fillRandom(sourceFile, fileSize.toBytes());
  }
  
  @AfterClass
  public void tearDown() {
    sourceFile.delete();
    LOG.info("Deleted %s", sourceFile.getAbsolutePath());
  }
  
  @BeforeMethod
  public void setUp() throws IOException {
    this.cacheDirectory = createTempDirectory("carrot_cache").toUri();
    try {
      this.fs = cachingFileSystem();
    } catch (URISyntaxException e) {
      // TODO Auto-generated catch block
      e.printStackTrace();
    } catch (IOException e) {
      // TODO Auto-generated catch block
      e.printStackTrace();
    }
  }
  
  @AfterMethod 
  public void close() throws IOException {
    CarrotCachingFileSystem cfs = (CarrotCachingFileSystem) fs;
    cfs.getCache().dispose();
    checkState(cacheDirectory != null);
    TestUtils.deletePathRecursively(cacheDirectory.getPath());
    LOG.info("Deleted %s", cacheDirectory);
  }
  
  @Test
  public void testFileSystem() throws Exception {
    byte[] buffer = new byte[1000];
    int read = readFully(1000, buffer, 0, buffer.length);
    assertTrue(buffer.length == read);
    
  }
  
  private int readFully(long position, byte[] buffer, int offset, int length)
      throws Exception
{
    CacheQuota cacheQuota = new CacheQuota("test.table", Optional.of(DataSize.succinctDataSize(5, KILOBYTE)));

    try (FSDataInputStream stream = fs.openFile(
          new Path(sourceFile.getAbsolutePath()),
          new HiveFileContext(
                  true,
                  cacheQuota,
                  Optional.empty(),
                  OptionalLong.of(fileSize.toBytes()),
                  OptionalLong.of(offset),
                  OptionalLong.of(length),
                  0,
                  false))) {
      return stream.read(position, buffer, offset, length);
  }
}
  
  private CarrotCachingFileSystem cachingFileSystem()
      throws URISyntaxException, IOException {
    
    CacheConfig cacheConfig = new CacheConfig()
        .setCacheType(CARROT)
        .setCachingEnabled(true)
        .setBaseDirectory(cacheDirectory);
    CarrotCacheConfig carrotCacheConfig = new CarrotCacheConfig()
        .setMaxCacheSize(cacheSize)
        .setDataSegmentSize(cacheSegmentSize)
        .setCacheAdmissionControllerEnabled(true)
        .setCacheAdmissionQueueSizeRatio(0.2)
        .setSLRUEvictionInsertionPoint(6)
        .setStartIndexSlotsPower(4)
        .setMetricsCollectionEnabled(true);
    
    Configuration configuration = TestUtils.getHdfsConfiguration(cacheConfig, carrotCacheConfig);
    ExtendedFileSystem testingFileSystem = new TestingFileSystem(configuration);
    URI uri = new URI("carrot://test:8020/");
    // 
    CarrotCachingFileSystem cachingFileSystem = new CarrotCachingFileSystem(testingFileSystem, uri, true);
    cachingFileSystem.initialize(uri, configuration);
    return cachingFileSystem;
  }
  
  private static class TestingFileSystem extends ExtendedFileSystem {

    TestingFileSystem(Configuration configuration) {
      setConf(configuration);
    }

    @Override
    public FileStatus getFileStatus(Path path) {
      return null;
    }

    @Override
    public URI getUri() {
      return null;
    }

    @Override
    public FSDataInputStream open(Path path, int bufferSize) {
      path = Path.getPathWithoutSchemeAndAuthority(path);
      String filePath = path.toString();
      try {
        RandomAccessFile raf = new RandomAccessFile(filePath, "rw");
        return new FSDataInputStream(new RandomAccessFileInputStream(raf));
      } catch (IOException e) {
        
      }
      return null;
    }

    @Override
    public FSDataOutputStream create(Path path, FsPermission permission, boolean overwrite,
        int bufferSize, short replication, long blockSize, Progressable progress) {
      return null;
    }

    @Override
    public FSDataOutputStream append(Path path, int bufferSize, Progressable progress) {
      return null;
    }

    @Override
    public boolean rename(Path source, Path destination) {
      return false;
    }

    @Override
    public boolean delete(Path path, boolean recursive) {
      return false;
    }

    @Override
    public FileStatus[] listStatus(Path path) {
      return new FileStatus[0];
    }

    @Override
    public void setWorkingDirectory(Path directory) {
    }

    @Override
    public Path getWorkingDirectory() {
      return null;
    }

    @Override
    public boolean mkdirs(Path path, FsPermission permission) {
      return false;
    }

    @Override
    public short getDefaultReplication() {
      return 0;
    }

    @Override
    public short getDefaultReplication(Path path) {
      return 10;
    }

    @Override
    public long getDefaultBlockSize() {
      return 0;
    }

    @Override
    public long getDefaultBlockSize(Path path) {
      return 1024L;
    }
  }
}
