package com.tinkerpop.gremlin.server;

import com.codahale.metrics.Meter;
import com.tinkerpop.gremlin.server.util.MetricManager;
import io.netty.buffer.ByteBuf;
import io.netty.buffer.Unpooled;
import io.netty.channel.ChannelFuture;
import io.netty.channel.ChannelFutureListener;
import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.SimpleChannelInboundHandler;
import io.netty.handler.codec.http.DefaultFullHttpResponse;
import io.netty.handler.codec.http.FullHttpRequest;
import io.netty.handler.codec.http.FullHttpResponse;
import io.netty.handler.codec.http.websocketx.CloseWebSocketFrame;
import io.netty.handler.codec.http.websocketx.PingWebSocketFrame;
import io.netty.handler.codec.http.websocketx.PongWebSocketFrame;
import io.netty.handler.codec.http.websocketx.TextWebSocketFrame;
import io.netty.handler.codec.http.websocketx.WebSocketFrame;
import io.netty.handler.codec.http.websocketx.WebSocketServerHandshaker;
import io.netty.handler.codec.http.websocketx.WebSocketServerHandshakerFactory;
import io.netty.util.CharsetUtil;
import io.netty.util.ReferenceCountUtil;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import static com.codahale.metrics.MetricRegistry.name;
import static io.netty.handler.codec.http.HttpHeaders.Names.CONTENT_TYPE;
import static io.netty.handler.codec.http.HttpHeaders.Names.HOST;
import static io.netty.handler.codec.http.HttpHeaders.isKeepAlive;
import static io.netty.handler.codec.http.HttpHeaders.setContentLength;
import static io.netty.handler.codec.http.HttpMethod.GET;
import static io.netty.handler.codec.http.HttpResponseStatus.BAD_REQUEST;
import static io.netty.handler.codec.http.HttpResponseStatus.FORBIDDEN;
import static io.netty.handler.codec.http.HttpResponseStatus.NOT_FOUND;
import static io.netty.handler.codec.http.HttpResponseStatus.OK;
import static io.netty.handler.codec.http.HttpVersion.HTTP_1_1;

/**
 * Adapted from https://github.com/netty/netty/tree/netty-4.0.10.Final/example/src/main/java/io/netty/example/http/websocketx/server
 *
 * @author Stephen Mallette (http://stephen.genoprime.com)
 */
class RequestHandler extends SimpleChannelInboundHandler<Object> {
    private static final Logger logger = LoggerFactory.getLogger(GremlinServerHandler.class);
    static final Meter requestMeter = MetricManager.INSTANCE.getMeter(name(GremlinServer.class, "requests"));
    static final Meter errorMeter = MetricManager.INSTANCE.getMeter(name(GremlinServer.class, "errors"));
    private static final String websocketPath = "/gremlin";

    private WebSocketServerHandshaker handshaker;
    private final Settings settings;

    public RequestHandler(final Settings settings) {
        this.settings = settings;
    }

    @Override
    public void channelRead0(final ChannelHandlerContext ctx, final Object msg) throws Exception {

        if (msg instanceof FullHttpRequest) {
            handleHttpRequest(ctx, (FullHttpRequest) msg);
        } else if (msg instanceof WebSocketFrame) {
            handleWebSocketFrame(ctx, (WebSocketFrame) msg);
        }
    }

    @Override
    public void channelRead(ChannelHandlerContext ctx, Object msg) throws Exception {
        boolean release = true;
        try {
            channelRead0(ctx, msg);
        } finally {
            if (release) {
                ReferenceCountUtil.release(msg);
            }
        }

    }

    /*
    @Override
    public void channelReadComplete(final ChannelHandlerContext ctx) throws Exception {
        ctx.flush();
    }
    */

    private void handleHttpRequest(final ChannelHandlerContext ctx, final FullHttpRequest req) throws Exception {
        // Handle a bad request.
        if (!req.getDecoderResult().isSuccess()) {
            sendHttpResponse(ctx, req, new DefaultFullHttpResponse(HTTP_1_1, BAD_REQUEST));
            return;
        }

        // Allow only GET methods.
        if (req.getMethod() != GET) {
            sendHttpResponse(ctx, req, new DefaultFullHttpResponse(HTTP_1_1, FORBIDDEN));
            return;
        }

        final String uri = req.getUri();
        if ("/".equals(uri)) {
            ByteBuf content = GremlinServerIndexPage.getContent(getWebSocketLocation(req));
            FullHttpResponse res = new DefaultFullHttpResponse(HTTP_1_1, OK, content);

            res.headers().set(CONTENT_TYPE, "text/html; charset=UTF-8");
            setContentLength(res, content.readableBytes());

            sendHttpResponse(ctx, req, res);
            return;
        }

        if ("/favicon.ico".equals(req.getUri())) {
            FullHttpResponse res = new DefaultFullHttpResponse(HTTP_1_1, NOT_FOUND);
            sendHttpResponse(ctx, req, res);
            return;
        }

        if (uri.startsWith(websocketPath)) {
            // Web socket handshake
            final WebSocketServerHandshakerFactory wsFactory = new WebSocketServerHandshakerFactory(
                    getWebSocketLocation(req), null, false);
            handshaker = wsFactory.newHandshaker(req);
            if (handshaker == null) {
                WebSocketServerHandshakerFactory.sendUnsupportedWebSocketVersionResponse(ctx.channel());
            } else {
                final ChannelFuture f = handshaker.handshake(ctx.channel(), req);
                f.await();
                System.out.println("wtf");
            }
        }
    }

    private void handleWebSocketFrame(final ChannelHandlerContext ctx, final WebSocketFrame frame) {
        requestMeter.mark();

        // Check for closing frame
        if (frame instanceof CloseWebSocketFrame)
            handshaker.close(ctx.channel(), (CloseWebSocketFrame) frame.retain());
        else if (frame instanceof PingWebSocketFrame)
            ctx.channel().write(new PongWebSocketFrame(frame.content().retain()));
        else if (frame instanceof PongWebSocketFrame) { } // nothing to do
        else if (frame instanceof TextWebSocketFrame) {
            ctx.fireChannelRead(((TextWebSocketFrame) frame).text());
        } else {
            // gets caught by the exceptionCaught method on this handler.
            throw new UnsupportedOperationException(String.format("%s frame types not supported", frame.getClass()
                    .getName()));
        }
    }

    private static void sendHttpResponse(final ChannelHandlerContext ctx,
                                         final FullHttpRequest req, final FullHttpResponse res) {
        // Generate an error page if response getStatus code is not OK (200).
        if (res.getStatus().code() != 200) {
            final ByteBuf buf = Unpooled.copiedBuffer(res.getStatus().toString(), CharsetUtil.UTF_8);
            res.content().writeBytes(buf);
            buf.release();
            setContentLength(res, res.content().readableBytes());
        }

        // Send the response and close the connection if necessary.
        final ChannelFuture f = ctx.channel().writeAndFlush(res);
        if (!isKeepAlive(req) || res.getStatus().code() != 200) {
            f.addListener(ChannelFutureListener.CLOSE);
        }
    }

    @Override
    public void exceptionCaught(final ChannelHandlerContext ctx, final Throwable cause) throws Exception {
        // this only happens if an exception fires that isn't handled.  A good example would be if a frame
        // was sent that was not covered.  bad stuff if we get here.
        logger.error("Message handler caught an exception fatal to this request. Closing connection.", cause);
        errorMeter.mark();
        ctx.close();
    }

    private String getWebSocketLocation(FullHttpRequest req) {
        return "ws://" + req.headers().get(HOST) + websocketPath;
    }

    private static final class GremlinServerIndexPage {
        private GremlinServerIndexPage() {}

        public static ByteBuf getContent(String webSocketLocation) {
            final StringBuilder sb = new StringBuilder();
            sb.append("<html style=\"background-color:#111111\">");
            sb.append("<head><meta charset=\"UTF-8\"><title>Gremlin Server</title></head>");
            sb.append("<body>");
            sb.append("<div align=\"center\"><a href=\"http://tinkerpop.com\"><img style=\"width:300px\" src=\"https://raw2.github.com/tinkerpop/homepage/master/images/tinkerpop3-splash.png\"/></a></div>");
            sb.append("<div align=\"center\">");
            sb.append("<h3 style=\"color:#B5B5B5\">Gremlin Server - " + com.tinkerpop.gremlin.Tokens.VERSION + "</h3>");
            sb.append("<p>");
            sb.append(webSocketLocation);
            sb.append("</p>");
            sb.append("</div>");
            sb.append("</body>");
            sb.append("</html>");


            return Unpooled.copiedBuffer(sb.toString(), CharsetUtil.US_ASCII);
        }
    }
}