// Copyright 2020 IBM
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
// http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package com.cloudant.search3;

import io.netty.bootstrap.ServerBootstrap;
import io.netty.channel.Channel;
import io.netty.channel.ChannelHandler;
import io.netty.channel.ChannelOption;
import io.netty.channel.EventLoopGroup;
import io.netty.channel.nio.NioEventLoopGroup;
import io.netty.channel.socket.nio.NioServerSocketChannel;
import io.netty.handler.logging.LogLevel;
import io.netty.handler.logging.LoggingHandler;
import io.netty.handler.ssl.ClientAuth;
import io.netty.handler.ssl.SslContext;
import io.netty.handler.ssl.SslContextBuilder;
import java.io.File;
import java.util.Collections;
import java.util.List;
import javax.net.ssl.SSLException;
import org.apache.commons.configuration2.Configuration;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

public abstract class AbstractServer {

  private final Logger logger = LogManager.getLogger(getClass());
  protected final Configuration configuration;

  private EventLoopGroup bossGroup;
  private EventLoopGroup workerGroup;
  private Channel ch;

  protected AbstractServer(final Configuration configuration) {
    this.configuration = configuration;
  }

  public final void start() throws Exception {
    final int bossGroupThreadCount = configuration.getInt("boss_group_thread_count", 1);
    final int soBacklog = configuration.getInt("so_backlog", 1024);
    final SslContext sslContext = configureSslContext();
    final ChannelHandler channelHandler = configureChannelHandler(sslContext);

    bossGroup = new NioEventLoopGroup(bossGroupThreadCount);
    workerGroup = new NioEventLoopGroup();
    final ServerBootstrap b = new ServerBootstrap();
    b.option(ChannelOption.SO_BACKLOG, soBacklog);
    b.group(bossGroup, workerGroup)
        .channel(NioServerSocketChannel.class)
        .handler(new LoggingHandler(LogLevel.INFO))
        .childHandler(channelHandler);

    final int port = configuration.getInt("port");
    ch = b.bind(port).sync().channel();

    logger.info("Server started on port {} {} TLS.", port, sslContext != null ? "with" : "without");
  }

  public final void stop() throws InterruptedException {
    ch.closeFuture().sync();
    bossGroup.shutdownGracefully();
    workerGroup.shutdownGracefully();
  }

  protected abstract ChannelHandler configureChannelHandler(final SslContext sslContext);

  private SslContext configureSslContext() throws SSLException {
    if (!configuration.getBoolean("tls.enabled", false)) {
      return null;
    }

    final List<String> defaultTlsProtocols = Collections.singletonList("TLSv1.2");
    final List<String> tlsProtocols =
        configuration.getList(String.class, "tls.protocols", defaultTlsProtocols);

    final File certChainFile = new File(configuration.getString("tls.cert_file"));
    // Key needs to be in PKCS8 format for Netty for some bizarre reason.
    final File privateKeyFile = new File(configuration.getString("tls.key_file"));
    final File clientCAFile = new File(configuration.getString("tls.ca_file"));
    final ClientAuth clientAuth =
        configuration.get(ClientAuth.class, "tls.client_auth", ClientAuth.REQUIRE);
    return SslContextBuilder.forServer(certChainFile, privateKeyFile)
        .protocols(tlsProtocols)
        .trustManager(clientCAFile)
        .clientAuth(clientAuth)
        .build();
  }
}
