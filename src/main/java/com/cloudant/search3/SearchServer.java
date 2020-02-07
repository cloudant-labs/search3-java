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

import static io.netty.buffer.Unpooled.wrappedBuffer;
import static io.netty.handler.codec.http.HttpHeaderNames.CONNECTION;
import static io.netty.handler.codec.http.HttpHeaderNames.CONTENT_LENGTH;
import static io.netty.handler.codec.http.HttpHeaderNames.CONTENT_TYPE;
import static io.netty.handler.codec.http.HttpHeaderValues.APPLICATION_OCTET_STREAM;
import static io.netty.handler.codec.http.HttpHeaderValues.CLOSE;
import static io.netty.handler.codec.http.HttpHeaderValues.KEEP_ALIVE;
import static io.netty.handler.codec.http.HttpMethod.POST;
import static io.netty.handler.codec.http.HttpResponseStatus.OK;

import com.cloudant.search3.grpc.Search3.AnalyzeRequest;
import com.cloudant.search3.grpc.Search3.DocumentDeleteRequest;
import com.cloudant.search3.grpc.Search3.DocumentUpdateRequest;
import com.cloudant.search3.grpc.Search3.ErrorResponse;
import com.cloudant.search3.grpc.Search3.ErrorResponse.Type;
import com.cloudant.search3.grpc.Search3.GroupSearchRequest;
import com.cloudant.search3.grpc.Search3.Index;
import com.cloudant.search3.grpc.Search3.SearchRequest;
import com.google.protobuf.Empty;
import com.google.protobuf.InvalidProtocolBufferException;
import com.google.protobuf.MessageLite;
import io.netty.buffer.ByteBuf;
import io.netty.buffer.ByteBufUtil;
import io.netty.channel.ChannelFuture;
import io.netty.channel.ChannelFutureListener;
import io.netty.channel.ChannelHandler;
import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.ChannelInitializer;
import io.netty.channel.ChannelPipeline;
import io.netty.channel.SimpleChannelInboundHandler;
import io.netty.channel.socket.SocketChannel;
import io.netty.handler.codec.http.DefaultFullHttpResponse;
import io.netty.handler.codec.http.FullHttpRequest;
import io.netty.handler.codec.http.FullHttpResponse;
import io.netty.handler.codec.http.HttpContentCompressor;
import io.netty.handler.codec.http.HttpContentDecompressor;
import io.netty.handler.codec.http.HttpObject;
import io.netty.handler.codec.http.HttpObjectAggregator;
import io.netty.handler.codec.http.HttpServerCodec;
import io.netty.handler.codec.http.HttpServerExpectContinueHandler;
import io.netty.handler.codec.http.HttpServerKeepAliveHandler;
import io.netty.handler.codec.http.HttpUtil;
import io.netty.handler.codec.http.HttpVersion;
import io.netty.handler.ssl.SslContext;
import io.netty.util.concurrent.DefaultEventExecutorGroup;
import io.netty.util.concurrent.EventExecutorGroup;
import java.io.IOException;
import org.apache.commons.configuration2.Configuration;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.apache.lucene.queryparser.classic.ParseException;

public class SearchServer extends AbstractServer {

  private static final Logger LOGGER = LogManager.getLogger();
  private static final String RPC_MESSAGE_TYPE = "rpc-message-type";
  private static final ByteBuf EMPTY = wrappedBuffer(Empty.getDefaultInstance().toByteArray());

  private class SearchServerHandler extends SimpleChannelInboundHandler<HttpObject> {

    private final Search search;

    private SearchServerHandler(final Search search) {
      this.search = search;
    }

    @Override
    public void channelReadComplete(final ChannelHandlerContext ctx) {
      ctx.flush();
    }

    @Override
    protected void channelRead0(final ChannelHandlerContext ctx, final HttpObject msg)
        throws Exception {
      if (msg instanceof FullHttpRequest) {
        final FullHttpRequest req = (FullHttpRequest) msg;

        if (!POST.equals(req.method())) {
          ctx.writeAndFlush(error(Type.CLIENT_ERROR, req.uri() + " method not allowed"));
          return;
        }

        final String messageType;
        final ByteBuf result;
        switch (req.uri()) {
          case "/Search/Delete":
            {
              final Index index = decode(Index.getDefaultInstance(), req);
              search.delete(index);
              messageType = "empty";
              result = EMPTY;
              break;
            }
          case "/Search/Info":
            {
              final Index index = decode(Index.getDefaultInstance(), req);
              messageType = "info_response";
              result = encode(search.info(index));
              break;
            }
          case "/Search/Search":
            {
              final SearchRequest request = decode(SearchRequest.getDefaultInstance(), req);
              messageType = "search_response";
              result = encode(search.search(request));
              break;
            }
          case "/Search/GroupSearch":
            {
              final GroupSearchRequest request =
                  decode(GroupSearchRequest.getDefaultInstance(), req);
              messageType = "group_search_response";
              result = encode(search.groupSearch(request));
              break;
            }
          case "/Search/UpdateDocument":
            {
              final DocumentUpdateRequest request =
                  decode(DocumentUpdateRequest.getDefaultInstance(), req);
              messageType = "session_response";
              result = encode(search.updateDocument(request));
              break;
            }
          case "/Search/DeleteDocument":
            {
              final DocumentDeleteRequest request =
                  decode(DocumentDeleteRequest.getDefaultInstance(), req);
              messageType = "session_response";
              result = encode(search.deleteDocument(request));
              break;
            }
          case "/Search/Analyze":
            {
              final AnalyzeRequest request = decode(AnalyzeRequest.getDefaultInstance(), req);
              messageType = "analyze_response";
              result = encode(search.analyze(request));
              break;
            }
          default:
            {
              ctx.writeAndFlush(error(Type.CLIENT_ERROR, req.uri() + " not recognized"));
              return;
            }
        }

        boolean keepAlive = HttpUtil.isKeepAlive(req);
        FullHttpResponse response = new DefaultFullHttpResponse(req.protocolVersion(), OK, result);
        response
            .headers()
            .set(RPC_MESSAGE_TYPE, messageType)
            .set(CONTENT_TYPE, APPLICATION_OCTET_STREAM)
            .setInt(CONTENT_LENGTH, response.content().readableBytes());

        if (keepAlive) {
          if (!req.protocolVersion().isKeepAliveDefault()) {
            response.headers().set(CONNECTION, KEEP_ALIVE);
          }
        } else {
          // Tell the client we're going to close the connection.
          response.headers().set(CONNECTION, CLOSE);
        }

        ChannelFuture f = ctx.writeAndFlush(response);

        if (!keepAlive) {
          f.addListener(ChannelFutureListener.CLOSE);
        }
      }
    }

    @Override
    public void exceptionCaught(final ChannelHandlerContext ctx, final Throwable cause) {
      try {
        final Type type;
        if (cause instanceof ParseException) {
          type = Type.CLIENT_ERROR;
        } else if (cause instanceof IOException) {
          type = Type.SERVER_ERROR;
        } else if (cause instanceof SessionMismatchException) {
          type = Type.SESSION_MISMATCH;
        } else if (cause instanceof IllegalArgumentException) {
          type = Type.CLIENT_ERROR;
        } else if (cause instanceof IllegalStateException) {
          type = Type.CLIENT_ERROR;
        } else {
          type = Type.UNKNOWN;
          LOGGER.catching(cause);
        }
        ctx.writeAndFlush(error(type, cause.getMessage()));
      } finally {
        ctx.close();
      }
    }

    @SuppressWarnings("unchecked")
    private <T extends MessageLite> T decode(final T prototype, final FullHttpRequest req)
        throws InvalidProtocolBufferException {
      // cribbed from ProtobufDecoder.decode().
      final ByteBuf msg = req.content();
      final byte[] array;
      final int offset;
      final int length = msg.readableBytes();
      if (msg.hasArray()) {
        array = msg.array();
        offset = msg.arrayOffset() + msg.readerIndex();
      } else {
        array = ByteBufUtil.getBytes(msg, msg.readerIndex(), length, false);
        offset = 0;
      }
      return (T) prototype.getParserForType().parseFrom(array, offset, length);
    }

    private ByteBuf encode(final MessageLite msg) {
      return wrappedBuffer(msg.toByteArray());
    }

    private FullHttpResponse error(final Type type, final String reason) {
      final ErrorResponse error =
          ErrorResponse.newBuilder().setType(type).setReason(reason).build();
      final ByteBuf body = wrappedBuffer(error.toByteArray());
      final FullHttpResponse result = new DefaultFullHttpResponse(HttpVersion.HTTP_1_1, OK, body);
      result
          .headers()
          .set(RPC_MESSAGE_TYPE, "error_response")
          .set(CONTENT_TYPE, APPLICATION_OCTET_STREAM)
          .setInt(CONTENT_LENGTH, result.content().readableBytes());
      return result;
    }
  }

  private class SearchServerInitializer extends ChannelInitializer<SocketChannel> {

    private final EventExecutorGroup searchGroup;

    private final SslContext sslContext;

    public SearchServerInitializer(final SslContext sslContext) {
      this.sslContext = sslContext;
      final int concurrency = configuration.getInt("concurrency", 16);
      this.searchGroup = new DefaultEventExecutorGroup(concurrency);
    }

    @Override
    public void initChannel(SocketChannel ch) {
      final ChannelPipeline p = ch.pipeline();
      if (sslContext != null) {
        p.addLast(sslContext.newHandler(ch.alloc()));
      }

      // http
      p.addLast("httpCodec", new HttpServerCodec());
      final int maxContentLength = configuration.getInt("max_request_length", 10485760);
      p.addLast("httpAggregator", new HttpObjectAggregator(maxContentLength));

      // inbound
      p.addLast("httpExpectContinue", new HttpServerExpectContinueHandler());
      p.addLast("httpKeepAlive", new HttpServerKeepAliveHandler());
      p.addLast("httpDecompressor", new HttpContentDecompressor());

      // outbound
      p.addLast("httpCompressor", new HttpContentCompressor());

      // Search handler in a separate group to the I/O.
      p.addLast(searchGroup, "searchHandler", new SearchServerHandler(search));
    }
  }

  private Search search;

  public SearchServer(final Configuration configuration, final Search search) {
    super(configuration);
    this.search = search;
  }

  @Override
  protected ChannelHandler configureChannelHandler(final SslContext sslContext) {
    return new SearchServerInitializer(sslContext);
  }
}
