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

import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertTrue;
import static org.testng.Assert.fail;

import java.io.File;
import java.io.IOException;
import java.io.RandomAccessFile;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.nio.file.StandardCopyOption;

import org.apache.hadoop.fs.FSDataInputStream;
import org.testng.annotations.AfterMethod;
import org.testng.annotations.Test;

import com.carrot.cache.util.Utils;
import com.google.common.base.VerifyException;

import io.airlift.units.DataSize;
import io.airlift.units.DataSize.Unit;

@Test(singleThreaded = true)
public class TestCacheValidatingInputStream
{
    File f1;
    File f2;
    
    @AfterMethod(alwaysRun = true)
    public void tearDown()
            throws Exception
    {
       if (f1 != null) {
         f1.delete();
         f1 = null;
       }
       if (f2 != null) {
         f2.delete();
         f2 = null;
       }
    }
    
    @Test
    public void testValidateDataEnabledWithDataMismatchMemory()
            throws IOException
    {
       MemoryBufferSeekableStream streamOne = new MemoryBufferSeekableStream(100);
       MemoryBufferSeekableStream streamTwo = new MemoryBufferSeekableStream(100);
        try (
            FSDataInputStream dataTierInputStream = new FSDataInputStream(streamOne);
            FSDataInputStream fileInStream = new FSDataInputStream(streamTwo);
            CarrotCacheValidatingInputStream fileInputStream =
                new CarrotCacheValidatingInputStream(fileInStream, dataTierInputStream);)
        {
          
          byte[] buffer = new byte[100];
          fileInputStream.readFully(0, buffer, 0, buffer.length);
          fail("Data validation didn't work for mismatched data.");
        }
        catch (VerifyException ex) {
            assertEquals(ex.getMessage(), "corrupted buffer at offset 0 buffer size=100");
        }
    }

    @Test
    public void testValidateDataEnabledWithDataMismatchFile()
            throws IOException
    {
      long size = new DataSize(1, Unit.MEGABYTE).toBytes();
      f1 = TestUtils.createTempFile();
      TestUtils.fillRandom(f1, size);
      f2 = TestUtils.createTempFile();
      Path p1 = Paths.get(f1.getAbsolutePath());
      Path p2 = Paths.get(f2.getAbsolutePath());
      Files.copy(p1, p2, StandardCopyOption.REPLACE_EXISTING);
      long offset = Utils.SIZEOF_LONG * 1000;
      TestUtils.touchFileAt(f2, offset);
      
      
       RandomAccessFileInputStream streamOne = new RandomAccessFileInputStream(new RandomAccessFile(f1, "r"));
       RandomAccessFileInputStream streamTwo = new RandomAccessFileInputStream(new RandomAccessFile(f2, "r"));
        try (
            FSDataInputStream dataTierInputStream = new FSDataInputStream(streamOne);
            FSDataInputStream fileInStream = new FSDataInputStream(streamTwo);
            CarrotCacheValidatingInputStream fileInputStream =
                new CarrotCacheValidatingInputStream(fileInStream, dataTierInputStream);)
        {
          
          byte[] buffer = new byte[(int)offset];
          // This should be OK
          fileInputStream.readFully(0, buffer, 0, buffer.length);
          // This must fail
          fileInputStream.readFully(offset, buffer, 0, buffer.length);
          fail("Data validation didn't work for mismatched data.");
        }
        catch (VerifyException ex) {
            assertEquals(ex.getMessage(), "corrupted buffer at offset 0 buffer size=" + offset);
        }
    }
    
    @Test
    public void testValidateDataEnabledWithDataMatchedMemory()
            throws IOException
    {
      MemoryBufferSeekableStream streamOne = new MemoryBufferSeekableStream(100);
      MemoryBufferSeekableStream streamTwo = // Create copy of the first stream
          new MemoryBufferSeekableStream(streamOne.bufferAddress(), streamOne.length());
      
      try (
        FSDataInputStream dataTierInputStream = new FSDataInputStream(streamOne);
        FSDataInputStream fileInStream = new FSDataInputStream(streamTwo);
        CarrotCacheValidatingInputStream fileInputStream = 
            new CarrotCacheValidatingInputStream(fileInStream, dataTierInputStream);)
        {
          byte[] buffer = new byte[100];
          fileInputStream.readFully(0, buffer, 0, buffer.length);
          long ptr = streamOne.bufferAddress();
          long len = streamOne.length();
          assertTrue(Utils.compareTo(buffer, 0, buffer.length, ptr, (int) len) == 0);
        }
    }

    @Test
    public void testValidateDataEnabledWithDataMatchedFile()
            throws IOException
    {
      long size = new DataSize(1, Unit.MEGABYTE).toBytes();
      f1 = TestUtils.createTempFile();
      TestUtils.fillRandom(f1, size);
      f2 = TestUtils.createTempFile();
      Path p1 = Paths.get(f1.getAbsolutePath());
      Path p2 = Paths.get(f2.getAbsolutePath());
      Files.copy(p1, p2, StandardCopyOption.REPLACE_EXISTING);
      
      
       RandomAccessFileInputStream streamOne = new RandomAccessFileInputStream(new RandomAccessFile(f1, "r"));
       RandomAccessFileInputStream streamTwo = new RandomAccessFileInputStream(new RandomAccessFile(f2, "r"));
        try (
            FSDataInputStream dataTierInputStream = new FSDataInputStream(streamOne);
            FSDataInputStream fileInStream = new FSDataInputStream(streamTwo);
            )
        {
          CarrotCacheValidatingInputStream fileInputStream =
              new CarrotCacheValidatingInputStream(fileInStream, dataTierInputStream);
          int bufSize = 4096;
          byte[] buffer = new byte[bufSize];
          long read = 0;
          while(read < size) {
            int n = fileInputStream.read(read, buffer, 0, bufSize);
            read += n;
          }
        }
        catch (VerifyException ex) {
          fail("Data validation didn't work for mismatched data.");
        }
    }
    
    @Test
    public void testInteractionWithDataTierInputStreamMemory()
            throws IOException
    {
      MemoryBufferSeekableStream streamOne = new MemoryBufferSeekableStream(100);
      MemoryBufferSeekableStream streamTwo = // Create copy of the first stream
          new MemoryBufferSeekableStream(streamOne.bufferAddress(), streamOne.length());
      
      try (
          FSDataInputStream dataTierInputStream = new FSDataInputStream(streamOne);
          FSDataInputStream fileInStream = new FSDataInputStream(streamTwo);
          CarrotCacheValidatingInputStream validationEnabledInputStream = 
              new CarrotCacheValidatingInputStream(fileInStream, dataTierInputStream);)
        {

          // Seek on multiple positions on validationEnabledInputStream, it should also seek for same position in data tier
          validationEnabledInputStream.seek(2L);
          assertEquals(dataTierInputStream.getPos(), 2L);
          assertEquals(fileInStream.getPos(), 2L);
          validationEnabledInputStream.seek(1L);
          assertEquals(dataTierInputStream.getPos(), 1L);
          assertEquals(fileInStream.getPos(), 1L);
        }
    }
    
    @Test
    public void testInteractionWithDataTierInputStreamFile()
            throws IOException
    {
      long size = new DataSize(1, Unit.KILOBYTE).toBytes();
      f1 = TestUtils.createTempFile();
      TestUtils.fillRandom(f1, size);
      f2 = TestUtils.createTempFile();
      Path p1 = Paths.get(f1.getAbsolutePath());
      Path p2 = Paths.get(f2.getAbsolutePath());
      Files.copy(p1, p2, StandardCopyOption.REPLACE_EXISTING);
      
      
       RandomAccessFileInputStream streamOne = new RandomAccessFileInputStream(new RandomAccessFile(f1, "r"));
       RandomAccessFileInputStream streamTwo = new RandomAccessFileInputStream(new RandomAccessFile(f2, "r"));
        try (
            FSDataInputStream dataTierInputStream = new FSDataInputStream(streamOne);
            FSDataInputStream fileInStream = new FSDataInputStream(streamTwo);
            )
        {
          CarrotCacheValidatingInputStream validationEnabledInputStream =
              new CarrotCacheValidatingInputStream(fileInStream, dataTierInputStream);
          // Seek on multiple positions on validationEnabledInputStream, it should also seek for same position in data tier
          validationEnabledInputStream.seek(20L);
          assertEquals(dataTierInputStream.getPos(), 20L);
          assertEquals(fileInStream.getPos(), 20L);
          validationEnabledInputStream.seek(100L);
          assertEquals(dataTierInputStream.getPos(), 100L);
          assertEquals(fileInStream.getPos(), 100L);
        }
    }
    

}
