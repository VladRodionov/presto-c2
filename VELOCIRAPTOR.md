# Velociraptor

**Velociraptor** is the next evolution of PrestoDB hierarchical caching framework - [RaptorX](https://prestodb.io/blog/2021/02/04/raptorx):

## RaptorX

**RaptorX** has introduced five different caches each of them to solve a particular problem:

* **Metadata cache** - to keep table and partitions information locally to minimize round trips to the Hive MetaStore (Presto Coordinator).
* **File list cache** - to avoid lengthy ```listFiles()``` call to the remote file system (Presto Coordinator)
* **Parquet and ORC header/footers caches** - to store remote files index information (Presto Worker)
* **Fragment result cache** to store precalculated parts of a query plans for subsequent reuse (Presto Worker)
* **Data cache** - to cache data from remote file system on a local SSD (Presto Worker).

Although the initial reaction to the announcement was very favorable, there are some limitations which could be addressed:

1. **RaptorX** introduces 3 different caching technologies: 

- Guava cache for Metadata, File list, Parquet & ORC header/footers caches
- Custom disk - based for fragment result cache.
- Alluxio cache - for data caching
2. Guava cache does not scale well (because it uses JVM heap memory) and have high meta overhead , so three caches do not scale well and can introduce excessive JVM GC activity. Guava cache can only use JVM heap memory.
3. Both: fragment result cache and Alluxio are not SSD friendly - they incur very high DLWA (Device Level Write Amplification) due to frequent random writes in small blocks, which is the antipattern for SSD write access. This results in a poor SSD endurance in a such scenarious, in other words - SSD life span can be significantly reduced.
4. Both: fragment result cache and Alluxio data cache do not scale well beyound low millios of objects due to high meta overhead in JVM heap memory.
5. Out of 5 caches only Alluxio is restartable - content of all others fours is lost once Presto server is shutdown.
6. For data cache scan resistence is very important, but it seems that Alluxio is still looking for the right solution. 
7. Although not that critical, but single object - single local file approach of both: Data and Fragment result cache has its own drawback - maintanence problems. Managing millions files in local file system requires special approaches. For example, it takes a lot of time to execute simple shell commands, such ```rm -rf``` or ```ls -lf .```. 

## Velociraptor

**Velociraptor** introduces single caching solution for 3 (potentially - 4) caches - [Carrot Cache (C2)](https://github.com/VladRodionov/carrot-cache)  

**Velociraptor**, powered by **C2** solves all above problems:

1. It provides the single solution for most important caches: data page cache, file metadata cache (Parquet and ORC), file list cache and fragment result set cache (optional) - **C2**
2. It makes these caches much more scalable, because **C2** supports diferent mode of operations: RAM (offheap), SSD and Hybrid (RAM -> SSD). Velociraptor makes possible caching of billions of file statuses and meta inforamtion, which can significantly improve performance in a very large deployments.
3. Velociraptor significantly reduces JVM heap memory usage by moving caches metadata and data out of Java heap space providing additional room for query engine itself. It is very easy on JVM because it barely uses Java heap memory and produces virtually no object garbage during normal operation. 
4. It is SSD friendly, providing 20-30x times better SSD endurance compared to Alluxio or a home-grown SSD cache. This is because, all writes in **C2** are performed by large blocks, usually 256MB in size, (as opposed to 1MB writes in Alluxio). Writing data to SSD in 256MB blocks decreases DLWA by factor of 3-8x compared to writing data in 1MB blocks (Alluxio). Another very significant feature of **C2** - it utilizes pluggable Cache admission controller, which can significantly reduce data volume written to SSD while keeping hit ratio almost the same. Combination of log-structured storage and smart admission controller significantly reduces SSD wearing and increases its life span. Default admission controller shows significant reduction in write activity while keeping almost the same cache hit ratio.
5. **C2** provides several eviction algorithms out of box, some of them are scan-resistant, besides this, admission controller acts as the special suppression buffer for long scan operations. Long scans do not trash the cache, because they have no chances to reach the cache.
6. All major caches are restartable now, so it is safe to restart Presto server and have major caches up and runnning again.
7. **C2** number of files in the file system is manageable, usually in low thousands (not millions).
8. **C2** can scale to billions of objects in a single instance in both: RAM and SSD.
9. **C2** is very customizable, it allows to replace many components in the system: admission controllers, eviction algorithms, recycling selectors and some others.
10. **C2** is ML-ready, for example one can train custom admission controller for a particular workload, using some ML tools or libraries, then plugged it into **C2/Velociraptor** just by adding one line to the configuration file. 

## Current state of development

Data, Fragment results, File List have been replaced with **C2** and integrated into Presto. Metadata and Parquet/ORC caches are work in progress.

## How solid Carrot Cache is right now?

**C2** has been more than a year under intensive development and testing. It passes 12 hours stress tests in RAM, DISK, and Hybrid modes routinely. Current version is 0.4, 0.5 coming very soon.

## Prerequisites

- Clone the repository ```carrot-cache-demo```
 ```
 git clone https://github.com/VladRodionov/carrot-cache-demo.git
 ```
- Install ```carrot-cache``` artifact locally (its in /dist directory)

```
mvn install:install-file -Dfile=<path-to-file> -DgroupId=org.bigbase -DartifactId=carrot-cache -Dversion=0.5.0-SNAPSHOT
```
Note: **C2** does not support Java 8. Jav 8 has some serious bugs in the File nio package, which, unfortunately breaks the **C2** code during run-time. You can compile your project  with Java 8, but to run **C2** Java 11+ is required.

## Requirements to build Presto with Velociraptor are the same as for Presto itself

* Mac OS X or Linux
* Java 8 (159+)or higher 64-bit. Both Oracle JDK and OpenJDK are supported.
* Maven 3.3.9+ (for building)

## Building Presto + Velociraptor

1. Install **C2** first locally (read above)
2. Clone **velociraptor** project
```
git clone https://github.com/VladRodionov/velociraptor.git
```
3. Run the following command from the project root directory:

```
./mvnw clean install -DskipTests
```

## Velociraptor configuration

### Data cache 

In etc/catalog/hive.properties:

```
cache.enabled=true
cache.base-directory=file:///mnt/flash/data
# Carrot specific
cache.type=CARROT
# Maximum cache size
cache.carrot.max-cache-size=1500GB
# To export JMX metrics
cache.carrot.metrics-enabled=true
# JMX domann name, default is 'com.facebook.carrot'
cache.carrot.metrics-domain=some-name
# Data page is the minimum block of data which C2 caches, default is 1MB
cache.carrot.data-page-size=512KB
# I/O buffer size, default - 256kB
cache.carrot.io-buffer-size=1MB
# I/O pool size - it keeps I/O buffers for reuse, default - 32
ache.carrot.io-pool-size=128
# Data segment size, default is 128MB
cache.carrot.data-segment-size=256MB
# Cache eviction policy: SLRU or LRU, default is Segmented LRU
cache.carrot.eviction-policy=SLRU
# Recycling selector: LRC - always selects Least Recently Created data segment for recycling,
# MinAlive - always selects data segment which has minimum number of alive objects. Default: LRC
cache.carrot.recycling-selector=MinAlive
# Is admission controller enabled
cache.carrot.admission-controller-enabled=true
# The real number between 0.0 and 1.0. The less the number - the more restrictive admission is
# Default value is 0.5
cache.carrot.admission-queue-size-ratio=0.2

```
There are some other configuration parameters, you can check them out in : ```CarrotCacheConfig``` class.

## Fragment result cache 

In etc/config.properties:

```
fragment-result-cache.enabled=true
fragment-result-cache.base-directory=file:///mnt/flash/data
fragment-result-cache.cache-ttl=24h
fragment-result-cache.max-cache-size=500GB
hive.partition-statistics-based-optimization-enabled=true

# Carrot specific section
# Cache type : CARROT or FILE (default)
fragment-result-cache.type-name=CARROT
# Enable JMX metrics
carrot.fragment-result-cache.jmx-enabled=true
# JMX domann name, default is 'com.facebook.carrot'
carrot.fragment-result-cache.jmx-domain-name=some-name
# Is admission controller enabled 
carrot.fragment-result-cache.admission-controller-enabled=true
# The real number between 0.0 and 1.0. The less the number - the more restrictive admission is
# Default value is 0.5
carrot.fragment-result-cache.admission-queue-size-ratio=0.2
# Cache data segment size, default is 128MB
carrot.fragment-result-cache.data-segment-size=256MB
```
## File list cache

In etc/catalog/hive.properties

```
hive.file-status-cache-expire-time=24h
hive.file-status-cache-size=150GB
hive.file-status-cache-tables=*
# File list cach type : GUAVA (default) or CARROT
hive.file-status-cache-provider-type=CARROT
# Type of the Carrot cache: MEMORY or DISK
hive.carrot.file-status-cache-type=DISK
hive.carrot.file-status-cache-root-dir=file:///mnt/flash/data
hive.carrot.jmx-metrics-enabled=true
hive.carrot.jmx-metrics-domain-name=some-name
hive.carrot.data-segment-size=64MB
hive.carrot.admission-controller-enabled=true
hive.carrot.admission-controller-ratio=0.2
# Use 16 bytes MD5 hash instead of key (save space for long keys), default - false
hive.carrot.hash-for-keys-enabled=true

```
**In the DISK mode File List cache can scale to billions of files. Its purpose is to support very large data sets.**

**Very important:** The base data directory MUST BE THE SAME FOR ALL C2 CACHES in Velociraptor.

## Installation and deployment

How to install, deploy and run Presto server you can find on official [**Presto**](https://github.com/prestodb/presto) github page. Remember, only ```Java 11+``` is supported during run-time.

## Monitoring Velociraptor

When JMX monitoring is enabled, all **C2** caches expose their metrics under ```com.facebook.carrot``` domain name. You can use any tool, which can access JMX metrics from a local or a remote JVM process. I use JConsole during testing.

## Contact info

Fill free to contact me. I am open to any discussions, regarding this technology, sponsoships, contracts or job offers, which will allow me to continue working on both **Velociraptor** and **C2**. On a short notice, I can proide full **Velociraptor** binaries (with four major caches supported) for any interesting party to test and evaluate. 

Vladimir Rodionov
e-mail: vladrodionov@gmail.com


