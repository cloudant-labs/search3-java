// Copyright 2019 IBM
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package com.cloudant.search3;

import java.io.File;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import com.apple.foundationdb.Database;
import com.apple.foundationdb.FDB;

import io.grpc.Server;
import io.grpc.netty.shaded.io.grpc.netty.GrpcSslContexts;
import io.grpc.netty.shaded.io.grpc.netty.NettyServerBuilder;
import io.grpc.netty.shaded.io.netty.handler.ssl.ClientAuth;
import io.grpc.netty.shaded.io.netty.handler.ssl.SslContext;

public class Main {

    private static Logger LOGGER;

    public static void main(String[] args) throws Exception {
        System.setProperty("java.util.logging.manager", "org.apache.logging.log4j.jul.LogManager");
        LOGGER = LogManager.getLogger();

        final int port = Integer.parseInt(System.getProperty("port", "8443"));
        final int fdbApiVersion = Integer.parseInt(System.getProperty("fdbApiVersion", "600"));
        final File certChainFile = new File(System.getProperty("certChainFile", "cert.pem"));
        // Key needs to be in PKCS8 format for Netty for some bizarre reason.
        final File privateKeyFile = new File(System.getProperty("privateKeyFile", "key.pem"));
        final File clientCAFile = new File(System.getProperty("clientCAFile", "ca.pem"));

        FDB.selectAPIVersion(fdbApiVersion);
        final Database db = FDB.instance().open();

        final Search foo = new Search(db, FDBIndexWriterSearchHandler.factory());

        final SslContext sslContext = GrpcSslContexts.forServer(certChainFile, privateKeyFile)
                .trustManager(clientCAFile).clientAuth(ClientAuth.REQUIRE).build();

        final Server server = NettyServerBuilder.forPort(port).addService(foo).sslContext(sslContext).build();

        Runtime.getRuntime().addShutdownHook(new Thread() {
            @Override
            public void run() {
                server.shutdown();
            }
        });

        server.start();
        LOGGER.info("Server started.");
        server.awaitTermination();
        LOGGER.info("Server terminated.");
    }

}
