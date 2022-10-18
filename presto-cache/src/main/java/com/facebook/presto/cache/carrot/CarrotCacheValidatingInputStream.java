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

import org.apache.hadoop.fs.FSDataInputStream;

import com.carrot.cache.util.Utils;

import java.io.EOFException;
import java.io.IOException;

import static com.google.common.base.Verify.verify;
import static java.util.Objects.requireNonNull;

public class CarrotCacheValidatingInputStream
        extends FSDataInputStream
{
    private final FSDataInputStream inputStream;
    private final FSDataInputStream dataTierInputStream;

    public CarrotCacheValidatingInputStream(
            FSDataInputStream inputStream,
            FSDataInputStream dataTierInputStream)
    {
        super(inputStream);
        this.inputStream = requireNonNull(inputStream, "InputStream is null");
        this.dataTierInputStream = requireNonNull(dataTierInputStream, "DataTierInputStream is null");
    }

    @Override
    public int read()
            throws IOException
    {
        int outByte = inputStream.read();
        verify(dataTierInputStream.read() == outByte, "corrupted buffer at position " + getPos());
        return outByte;
    }

    @Override
    public int read(long position, byte[] buffer, int offset, int length)
            throws IOException
    {
        int bytes = inputStream.read(position, buffer, offset, length);
        byte[] validationBuffer = new byte[bytes];
        dataTierInputStream.readFully(position, validationBuffer, 0, bytes);
        verify(Utils.compareTo(buffer, offset, bytes, 
          validationBuffer, 0, bytes) == 0, "corrupted buffer at offset " 
        + offset + " buffer size=" + bytes);
        return bytes;
    }

    @Override
    public void readFully(long position, byte[] buffer, int offset, int length)
            throws IOException
    {
        int totalBytesRead = 0;
        while (totalBytesRead < length) {
            int bytesRead = read(
                    position + totalBytesRead,
                    buffer,
                    offset + totalBytesRead,
                    length - totalBytesRead);
            if (bytesRead == -1) {
                throw new EOFException();
            }
            totalBytesRead += bytesRead;
        }
    }

    @Override
    public void readFully(long position, byte[] buffer)
            throws IOException
    {
        readFully(position, buffer, 0, buffer.length);
    }

    @Override
    public void seek(long position)
            throws IOException
    {
        inputStream.seek(position);
        dataTierInputStream.seek(position);
    }

    @Override
    public long getPos()
            throws IOException
    {
        return inputStream.getPos();
    }

    @Override
    public boolean seekToNewSource(long target)
    {
        throw new UnsupportedOperationException();
    }
}
