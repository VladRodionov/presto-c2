/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.facebook.presto.cache.carrot.jmx;

import com.carrot.cache.Cache;
import com.facebook.presto.cache.carrot.CarrotCachingFileSystem;
import com.facebook.presto.cache.carrot.util.Stats;

public class CarrotCacheJMXSink implements CarrotCacheJMXSinkMBean{

  private CarrotCachingFileSystem fs;
  
  public CarrotCacheJMXSink(CarrotCachingFileSystem scfs) {
    this.fs = scfs;
  }
  
  @Override
  public String getremote_fs_uri() {
    return fs.getRemoteFSURI().toString();
  }

  @Override
  public int getdata_page_size() {
    return fs.getDataPageSize();
  }

  @Override
  public int getdata_prefetch_buffer_size() {
    return fs.getPrefetchBufferSize();
  }

  @Override
  public long gettotal_bytes_read() {
    Stats stats = fs.getStats();
    return stats.getTotalBytesRead();
  }

  @Override
  public long gettotal_bytes_read_remote_fs() {
    Stats stats = fs.getStats();
    return stats.getTotalBytesReadRemote();
  }

  @Override
  public long gettotal_bytes_read_data_cache() {
    Stats stats = fs.getStats();
    return stats.getTotalBytesReadDataCache();
  }

  @Override
  public long gettotal_bytes_read_prefetch() {
    Stats stats = fs.getStats();
    return stats.getTotalBytesReadPrefetch();
  }
  
  @Override
  public long gettotal_read_requests() {
    Stats stats = fs.getStats();
    return stats.getTotalReadRequests();
  }

  @Override
  public long gettotal_read_requests_remote_fs() {
    Stats stats = fs.getStats();
    return stats.getTotalReadRequestsFromRemote();
  }
  
  @Override
  public long gettotal_read_requests_data_cache() {
    Stats stats = fs.getStats();
    return stats.getTotalReadRequestsFromDataCache();
  }
  
  @Override
  public long gettotal_read_requests_prefetch() {
    Stats stats = fs.getStats();
    return stats.getTotalReadRequestsFromPrefetch();
  }
  
  @Override
  public long gettotal_scans_detected() {
    Stats stats = fs.getStats();
    return stats.getTotalScansDetected();
  }
  
  @Override
  public long getio_data_cache_read_avg_time() {
    Cache cache = CarrotCachingFileSystem.getCache();
    long duration = cache.getEngine().getTotalIOReadDuration() / 1000;
    return duration / cache.getTotalGets();
  }
  
  @Override
  public long getio_data_cache_read_avg_size() {
    Cache cache = CarrotCachingFileSystem.getCache();
    return cache.getTotalGetsSize() / cache.getTotalGets();
  }
  
  @Override
  public long getio_remote_fs_read_avg_time() {
    Stats stats = fs.getStats();
    long totalTime = stats.getTotalRemoteFSReadTime() / 1000;
    long requests = stats.getTotalReadRequestsFromRemote();
    return totalTime / requests;
  }
  
  @Override
  public long getio_remote_fs_read_avg_size() {
    Stats stats = fs.getStats();
    long totalBytes = stats.getTotalBytesReadRemote();
    long requests = stats.getTotalReadRequestsFromRemote();
    return totalBytes / requests;
  }
}
