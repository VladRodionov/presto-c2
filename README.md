# Velociraptor

**Velociraptor** is the next evolution of PrestoDB hierarchical caching framework - [RaptorX](https://prestodb.io/blog/2021/02/04/raptorx):

## RaptorX

**RaptorX** has introduced 5 different caches each of them to solve particular problem:

* **Metadata cache** - to keep table and partitions information locally at Presto Coordinator to avoid round trip to Hive Metaserver.
* **File list cache** - on Coordinator as well to avoid lengthy ```listFiles()``` call to the remote file system.
* **Parquet and ORC header/footers caches** - they store index information (its on the Worker)
* **Fragment result cache** on the Worker to store precalculated parts of a query plans for subsequent reuse
* **Data cache** - to cache data from remote file system on a local Worker's SSD.

Although the initial reaction to the announcement was very favorable, there are some limitations still which could be addressed:

1. RaptorX introduces 3 different caching technologies: 

- Guava cache for Metadata, File list, Parquet & ORC header/footers caches
- Custom disk - based for fragment result cache.
- Alluxio cache - for data caching
2. Guava cache does not scale well (because it uses JVM heap memory) and have high meta overhead , so three caches do not scale well and can introduce excessive JVM GC activity. Guava cache can only use JVM heap memory.
3. Both: fragment result cache and Alluxio are not SSD friendly - they incur very high DWLA (Device Level Write Amplification) due to frequent random writes in small blocks, which is the antipattern for SSD access. This results in a poor SSD endurance in a such scenarious.
4. Out of 5 caches only Alluxio is restartable - content of all others fours is lost once presto server is shutdown.
5. For data cache scan resistence is very important, but it seems that Alluxio is still looking for the right solution. 
6. Although not that critical, but single object - single local file approach of both: Data and Fragment result cache has its own drawbaks - maintanence nightmare. Managing millions files in local file system requires special approaches and someties can require a lot of time to perform a simple shell command, such ```rm -rf```. 

## Velociraptor

**Velociraptor** introduces the single caching solution for all 5 caches - [Carrot Cache (C2)](https://github.com/VladRodionov/carrot-cache)  

**Velociraptor**, powered by C2 solves all above problems:

1. It provides the single solution for all 5 caches - C2
2. It makes all 5 caches much more scalable, because C2 supports diferent mode of operations: RAM (offheap), SSD and Hybrid (RAM -> SSD)
3. Its is very easy on JVM because it barely uses Java heap memory and produces virtually no object garbage during normal operation. C2 does not use JVM heap cache and does not affect JVM GC at all.
4. Its SSD friendly, providing 20-30x times better SSD endurance compared to Alluxio or a home-grown SSD cache. This is because, all writes in C2 are performed by large blocks, usually 256MB in size, (as opposed to 1MB writes in Alluxio). Only this results in 3-8x better DLWA (Device Level Write Amplification), another significant feature of **C2** - it utilizes pluggable Cache admission controller, which can significantly reduce data volume wriiten to SSD while keeping hit ratio almost the same. Combination of log-structured storage and smart admission controller significantly reduces SSD wearing and increases its life span.
5. C2 provides several eviction algorithms out of box, some of them are scan-resistant. 
6. ALL FIVE CACHES are restartable now, so it is safe to restart presto server now and have caches up and runnning again.
7. And number of files in the file system is manageable, usually in low thousands (not millions).
8. C2 is very customizable, it allows to replace many components in the system: admission controllers, eviction algorithms, recycling selectors. 
It is ML-ready, for example one can train custom admission controller for particular workload, using some ML tools or libraries, then plugged it into C2/Velociraptor. 

## Current state of development

Both SSD caches (data and fragment results cache) are replaced by C2 and intgerated into prestodb. Data cache underwent some testing on a real databases and provided good results, fragments results cache is still being tested. Metadata, File list and Parquet/ORC will follow shortly - they are much simpler in terms of integration efforts and will be finished shortly.

## How solid Carrot Cache is right now?

It has been 15 months under intensive development and testing for myself as a full - time job. It passes 12 hours stress tests in RAM, DISK, and Hybrid modes. Current version is 0.4, 0.5 coming very soon. Quite solid already.

## Prerequisits

You need to build carrot cache binaries first in [Carrot Cache (C2)](https://github.com/VladRodionov/carrot-cache). Pull or fork the project
and run:

```./mvn clean install -DskipTests```

This will install locally the needed **carrot-cache** artifact. You can use both java 8 and java 11 to build carrot cache, but *only java 11+ to run it*. Java 8 has some serious bugs in the File nio package, which, unfortunately breaks the C2 code during run-time. 

## Requirements to build presto with Velociraptor are the same as for Presto itself

* Mac OS X or Linux
* Java 8 (159+)or higher 64-bit. Both Oracle JDK and OpenJDK are supported.
* Maven 3.3.9+ (for building)

## Building Presto + Velociraptor

Presto is a standard Maven project. Simply run the following command from the project root directory:

    ./mvnw clean install -DskipTests

How to install, deploy and run Presto server you can find on official **Presto** github page. Remember, only ```Java 11+``` is supported during run-time.

## What am I expecting from PrestoDB community

1. Excitement :)
2. Sponsorship or contract - this is a significant effort and requries significant time investment to introduce Velociraptor to the **Presto** community.

On a short notice, I can proide full Velociraptor binaries (with all 5 caches supported) for any interesting party to test and evaluate. 

## Contact info

Fill free to contact me, I am open to any discussions, regarding this technology, sponsoships, contracts or job offers, which will allow me to continue working on both Velociraptor and C2.

Vladimir Rodionov
e-mail: vladrodionov@gmail.com


