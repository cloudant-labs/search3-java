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

public abstract class AbstractServer {

    private final int port;
    private final ChannelHandler channelHandler;

    private EventLoopGroup bossGroup;
    private EventLoopGroup workerGroup;
    private Channel ch;

    protected AbstractServer(final int port, final ChannelHandler channelHandler) {
        this.port = port;
        this.channelHandler = channelHandler;
    }

    public final void start() throws Exception {
        bossGroup = new NioEventLoopGroup(1);
        workerGroup = new NioEventLoopGroup();
        final ServerBootstrap b = new ServerBootstrap();
        b.option(ChannelOption.SO_BACKLOG, 1024);
        b.group(bossGroup, workerGroup)
          .channel(NioServerSocketChannel.class)
          .handler(new LoggingHandler(LogLevel.INFO))
          .childHandler(channelHandler);

        ch = b.bind(port).sync().channel();
    }

    public final void stop() throws InterruptedException {
        ch.closeFuture().sync();
        bossGroup.shutdownGracefully();
        workerGroup.shutdownGracefully();
    }

}
