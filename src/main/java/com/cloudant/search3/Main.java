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

import org.apache.commons.configuration2.Configuration;
import org.apache.commons.configuration2.builder.fluent.Configurations;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import io.grpc.Server;
import io.grpc.netty.shaded.io.grpc.netty.GrpcSslContexts;
import io.grpc.netty.shaded.io.grpc.netty.NettyServerBuilder;
import io.grpc.netty.shaded.io.netty.handler.ssl.ClientAuth;
import io.grpc.netty.shaded.io.netty.handler.ssl.SslContext;
import io.grpc.netty.shaded.io.netty.handler.ssl.SslContextBuilder;

public class Main {

    private static Logger LOGGER;

    public static void main(String[] args) throws Exception {
        final Configurations configs = new Configurations();
        final Configuration config = configs.properties(new File("search3.ini"));

        System.setProperty("java.util.logging.manager", "org.apache.logging.log4j.jul.LogManager");
        LOGGER = LogManager.getLogger();

        final boolean tlsEnabled = config.getBoolean("tls.enabled", true);

        final SslContext grpcSslContext;
        final SslContext metricsSslContext;
        if (tlsEnabled) {
            final File certChainFile = new File(config.getString("tls.cert_file"));
            // Key needs to be in PKCS8 format for Netty for some bizarre reason.
            final File privateKeyFile = new File(config.getString("tls.key_file"));
            final File clientCAFile = new File(config.getString("tls.ca_file"));

            grpcSslContext = GrpcSslContexts.forServer(certChainFile, privateKeyFile)
                    .protocols("TLSv1.2", "TLSv1.3").trustManager(clientCAFile).clientAuth(ClientAuth.REQUIRE).build();

            metricsSslContext = SslContextBuilder.forServer(certChainFile, privateKeyFile)
                    .protocols("TLSv1.2", "TLSv1.3").trustManager(clientCAFile).clientAuth(ClientAuth.REQUIRE).build();
        } else {
            grpcSslContext = null;
            metricsSslContext = null;
        }

        // Start metrics first.
        final int metricsPort = config.getInt("metrics.port", 1234);
        final MetricsServer metricsServer = new MetricsServer(metricsPort, metricsSslContext);
        LOGGER.info("Metrics Server started on port {} {} TLS.", metricsPort, tlsEnabled ? "with" : "without");
        metricsServer.start();

        final int grpcPort = config.getInt("listen.port");

        final Search foo = Search.create(config);
        final NettyServerBuilder builder = NettyServerBuilder.forPort(grpcPort).addService(foo);

        if (tlsEnabled) {
            builder.sslContext(grpcSslContext);
        }

        final Server server = builder.build();

        Runtime.getRuntime().addShutdownHook(new Thread() {
            @Override
            public void run() {
                server.shutdown();
            }
        });

        server.start();
        LOGGER.info("Server started on port {} {} TLS.", grpcPort, tlsEnabled ? "with" : "without");
        server.awaitTermination();
        LOGGER.info("Server terminated.");

        metricsServer.stop();
    }

}
