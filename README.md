Search3 - Java
==============

This is the java component of a new text search application built on top
of FoundationDB. The erlang component resides at:

https://github.com/cloudant-labs/search3-erl

The two components communicate via gRPC. This erlang application is a client
and sends messages to this java component which acts as the grpc server.
Messages are sent via generated stubs using

https://github.com/tsloughter/grpcbox.

FoundationDB, search3-erl, and search3-java all must be up in order for text
search to work.

While search3-java acts as grpc server to handle text search requests,
the actual indexing and search is conducted by

https://github.com/cloudant-labs/fdblucene

which is a wrapper on top of Lucene to store lucene indexes into
FoundationDB.

Configuration
-------------

* `src/main/resources/search3.ini` - grpc server configuration.

* `src/main/resources/log4j2.properties` - logging/output configuration.
The default is stdout

* `src/main/resources/cache.ccf` - configuration for JCS (Java Caching System).
Unless you want to change how fdblucene caches data, this can be left
alone.

* `src/proto/health.proto` - defines the grpc protocol for health
check messages.

* `src/proto/search3.proto` - defines the grpc protocol required for
text indexing and text searching.

Build
-----

Requires Apache Maven to build:

https://maven.apache.org/

Requires https://github.com/cloudant-labs/fdblucene to be built
and reside within your local maven repository.

Run `mvn clean install -DskipTests`


Tests
-----

Requires a local FoundationDB cluster

https://www.foundationdb.org/

to be installed and running.

Run `mvn test` to run the unit tests included in `src/test`.


Running the Server
------------------

Once the project is built, a `search3-1.0.0-SNAPSHOT.jar` should
reside in the `target` directory.

Simple run: `java -jar target/search3-1.0.0-SNAPSHOT.jar`

INFO  Main:65 - Metrics Server started on port 1234 without TLS.
INFO  LoggingHandler:99 - [id: 0x30afbc2d] REGISTERED
INFO  LoggingHandler:99 - [id: 0x30afbc2d] BIND: 0.0.0.0/0.0.0.0:1234
INFO  LoggingHandler:99 - [id: 0x30afbc2d, L:/0:0:0:0:0:0:0:0:1234] ACTIVE
INFO  Main:88 - Server started on port 8443 without TLS.

To enable TLS, you need to set the following search3.ini

tls.cert_file = <path to>/cert.pem
tls.key_file = <path to>/key.pem
tls.ca_file = <path to>/ca.pem
tls.enabled = true

A make rule, `make ca.pem`, is available to easily build the cert, key, and ca for test purposes.
