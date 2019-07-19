FROM adoptopenjdk/openjdk8:alpine-slim
ENV FDB_VERSION=6.1.8
ENV FDB_SERVER_VERSION=6.0.18
RUN wget -q https://www.foundationdb.org/downloads/$FDB_VERSION/linux/libfdb_c_$FDB_VERSION.so --output-document /usr/local/lib/libfdb_c.so
RUN ldconfig /usr/local/lib/
RUN mkdir /etc/foundationdb
ENV LD_LIBRARY_PATH=$LD_LIBRARY_PATH:/usr/local/lib/
RUN wget -q https://www.foundationdb.org/downloads/$FDB_SERVER_VERSION/linux/libfdb_c_$FDB_SERVER_VERSION.so --output-document /usr/local/lib/libfdb_c_$FDB_SERVER_VERSION.so
ENV FDB_NETWORK_OPTION_EXTERNAL_CLIENT_LIBRARY=/usr/local/lib/libfdb_c_$FDB_SERVER_VERSION.so
ENV FDB_CLUSTER_FILE=/etc/foundationdb/fdb.cluster
ADD search3-1.0.0-SNAPSHOT.jar search3.jar
