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

import static io.netty.handler.codec.http.HttpHeaderNames.CONNECTION;
import static io.netty.handler.codec.http.HttpHeaderNames.CONTENT_LENGTH;
import static io.netty.handler.codec.http.HttpHeaderNames.CONTENT_TYPE;
import static io.netty.handler.codec.http.HttpHeaderValues.CLOSE;
import static io.netty.handler.codec.http.HttpHeaderValues.KEEP_ALIVE;
import static io.netty.handler.codec.http.HttpResponseStatus.OK;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.OutputStreamWriter;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

import org.apache.commons.configuration2.Configuration;

import io.netty.buffer.ByteBuf;
import io.netty.buffer.Unpooled;
import io.netty.channel.ChannelFuture;
import io.netty.channel.ChannelFutureListener;
import io.netty.channel.ChannelHandler;
import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.ChannelInitializer;
import io.netty.channel.ChannelPipeline;
import io.netty.channel.SimpleChannelInboundHandler;
import io.netty.channel.socket.SocketChannel;
import io.netty.handler.codec.http.DefaultFullHttpResponse;
import io.netty.handler.codec.http.FullHttpResponse;
import io.netty.handler.codec.http.HttpObject;
import io.netty.handler.codec.http.HttpRequest;
import io.netty.handler.codec.http.HttpServerCodec;
import io.netty.handler.codec.http.HttpServerExpectContinueHandler;
import io.netty.handler.codec.http.HttpUtil;
import io.netty.handler.codec.http.QueryStringDecoder;
import io.netty.handler.ssl.SslContext;
import io.prometheus.client.CollectorRegistry;
import io.prometheus.client.exporter.common.TextFormat;
import io.prometheus.client.hotspot.DefaultExports;

public class MetricsServer extends AbstractServer {

    private static class MetricsServerHandler extends SimpleChannelInboundHandler<HttpObject> {

        @Override
        public void channelReadComplete(ChannelHandlerContext ctx) {
            ctx.flush();
        }

        @Override
        public void channelRead0(ChannelHandlerContext ctx, HttpObject msg) {
            if (msg instanceof HttpRequest) {
                HttpRequest req = (HttpRequest) msg;

                final ByteBuf metrics = dump(req);

                boolean keepAlive = HttpUtil.isKeepAlive(req);
                FullHttpResponse response = new DefaultFullHttpResponse(req.protocolVersion(), OK,
                        metrics);
                response.headers().set(CONTENT_TYPE, TextFormat.CONTENT_TYPE_004)
                        .setInt(CONTENT_LENGTH, response.content().readableBytes());

                if (keepAlive) {
                    if (!req.protocolVersion().isKeepAliveDefault()) {
                        response.headers().set(CONNECTION, KEEP_ALIVE);
                    }
                } else {
                    // Tell the client we're going to close the connection.
                    response.headers().set(CONNECTION, CLOSE);
                }

                ChannelFuture f = ctx.write(response);

                if (!keepAlive) {
                    f.addListener(ChannelFutureListener.CLOSE);
                }
            }
        }

        private ByteBuf dump(final HttpRequest req) {
            final ByteArrayOutputStream bos = new ByteArrayOutputStream();
            final OutputStreamWriter writer = new OutputStreamWriter(bos);

            final CollectorRegistry registry = CollectorRegistry.defaultRegistry;
            try {
                TextFormat.write004(writer, registry.filteredMetricFamilySamples(parseQuery(req)));
                writer.flush();
                writer.close();
            } catch (final IOException e) {

            }

            return Unpooled.wrappedBuffer(bos.toByteArray());
        }

        @Override
        public void exceptionCaught(ChannelHandlerContext ctx, Throwable cause) {
            cause.printStackTrace();
            ctx.close();
        }

        private Set<String> parseQuery(final HttpRequest req) {
            final QueryStringDecoder decoder = new QueryStringDecoder(req.uri());
            final Map<String, List<String>> parameters = decoder.parameters();
            if (!parameters.containsKey("name[]")) {
                return Collections.emptySet();
            }
            return new HashSet<String>(parameters.get("name[]"));
        }

    }

    private static class MetricsServerInitializer extends ChannelInitializer<SocketChannel> {
        private final SslContext sslContext;

        public MetricsServerInitializer(SslContext sslContext) {
            this.sslContext = sslContext;
        }

        @Override
        public void initChannel(final SocketChannel ch) {
            ChannelPipeline p = ch.pipeline();
            if (sslContext != null) {
                p.addLast(sslContext.newHandler(ch.alloc()));
            }
            p.addLast(new HttpServerCodec());
            p.addLast(new HttpServerExpectContinueHandler());
            p.addLast(new MetricsServerHandler());
        }

    }

    public MetricsServer(final Configuration configuration) {
        super(configuration);
        // add included collectors
        DefaultExports.initialize();
    }

    @Override
    protected ChannelHandler configureChannelHandler(final SslContext sslContext) {
        return new MetricsServerInitializer(sslContext);
    }

}
