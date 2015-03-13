package reservoir.example.server;

import static io.netty.handler.codec.http.HttpHeaders.Names.CONNECTION;
import static io.netty.handler.codec.http.HttpHeaders.Names.CONTENT_LENGTH;
import static io.netty.handler.codec.http.HttpHeaders.Names.CONTENT_TYPE;
import static io.netty.handler.codec.http.HttpResponseStatus.OK;
import static io.netty.handler.codec.http.HttpVersion.HTTP_1_1;
import io.netty.buffer.Unpooled;
import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.ChannelInboundHandlerAdapter;
import io.netty.channel.SimpleChannelInboundHandler;
import io.netty.handler.codec.http.DefaultFullHttpResponse;
import io.netty.handler.codec.http.FullHttpResponse;
import io.netty.handler.codec.http.HttpContent;
import io.netty.handler.codec.http.HttpHeaders;
import io.netty.handler.codec.http.HttpHeaders.Values;
import io.netty.handler.codec.http.HttpMethod;
import io.netty.handler.codec.http.HttpObject;
import io.netty.handler.codec.http.HttpRequest;
import io.netty.handler.codec.http.LastHttpContent;
import io.netty.handler.codec.http.multipart.Attribute;
import io.netty.handler.codec.http.multipart.DefaultHttpDataFactory;
import io.netty.handler.codec.http.multipart.FileUpload;
import io.netty.handler.codec.http.multipart.HttpDataFactory;
import io.netty.handler.codec.http.multipart.HttpPostRequestDecoder;
import io.netty.handler.codec.http.multipart.HttpPostRequestDecoder.EndOfDataDecoderException;
import io.netty.handler.codec.http.multipart.InterfaceHttpData;
import io.netty.util.CharsetUtil;

import java.io.IOException;
import java.nio.charset.Charset;

public class HelloWorldServerHandler extends SimpleChannelInboundHandler<HttpObject> {
    private static final byte[] CONTENT = { 'H', 'e', 'l', 'l', 'o', ' ', 'W', 'o', 'r', 'l', 'd' };
    private boolean isChunked;
    private HttpDataFactory factory = new DefaultHttpDataFactory(DefaultHttpDataFactory.MINSIZE);
    private HttpPostRequestDecoder decoder;


    @Override
    public void channelActive(ChannelHandlerContext ctx) throws Exception {
        System.out.println("Connecting");
    }

    
    
    @Override
    public void channelRead0(ChannelHandlerContext ctx, HttpObject msg) throws Exception {
        if (msg instanceof HttpRequest) {
            HttpRequest req = (HttpRequest) msg;
            boolean keepAlive = HttpHeaders.isKeepAlive(req);
            // FullHttpResponse response = new DefaultFullHttpResponse(HTTP_1_1, OK,
            // Unpooled.wrappedBuffer(CONTENT));
            // response.headers().set(CONTENT_TYPE, "text/plain");
            // response.headers().set(CONTENT_LENGTH, response.content().readableBytes());
            isChunked = HttpHeaders.isTransferEncodingChunked(req);
            if (req.getMethod().equals(HttpMethod.POST)) {
                decoder = new HttpPostRequestDecoder(factory, req);
            } else if(req.getMethod().equals(HttpMethod.GET)){
                writeResponse(ctx);
            }
         }
        if (decoder != null) {
            if (msg instanceof HttpContent) {
                HttpContent httpContent = (HttpContent) msg;
                decoder.offer(httpContent);
                readHttpDataChunkByChunk();
                if (msg instanceof LastHttpContent) {
                    writeResponse(ctx);
                    isChunked = false;
                    reset();
                }
            }
        }
    }

    private void writeResponse(ChannelHandlerContext ctx) {
        FullHttpResponse response = new DefaultFullHttpResponse(HTTP_1_1, OK,
                Unpooled.wrappedBuffer(CONTENT));
        response.headers().set(CONTENT_TYPE, "text/plain");
        response.headers().set(CONTENT_LENGTH, response.content().readableBytes());
        response.headers().set(CONNECTION, Values.KEEP_ALIVE);
        ctx.write(response);
        ctx.flush();
    }



    void readHttpDataChunkByChunk() throws IOException {
        try {
            while (decoder.hasNext()) {
                InterfaceHttpData data = decoder.next();
                if (data != null) {
                    try {
                        // new value
                        if (data instanceof Attribute){
                            Attribute attr = (Attribute)data;
                            System.out.println(attr);
                        } else if (data instanceof FileUpload){
                            FileUpload fileUpload = (FileUpload)data;
                            System.out.println(fileUpload.content().toString(CharsetUtil.UTF_8));
                        }
                    } finally {
                        data.release();
                    }
                }
            }
        } catch (EndOfDataDecoderException e1) {
        }
    }

    @Override
    public void channelUnregistered(ChannelHandlerContext ctx) throws Exception {
        if (decoder != null) {
            decoder.cleanFiles();
        }
    }

    private void reset() {
        // destroy the decoder to release all resources
        decoder.destroy();
        decoder = null;
    }

    @Override
    public void exceptionCaught(ChannelHandlerContext ctx, Throwable cause) {
        cause.printStackTrace();
        ctx.close();
    }

}