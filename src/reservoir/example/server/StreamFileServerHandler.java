package reservoir.example.server;

import io.netty.channel.Channel;
import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.SimpleChannelInboundHandler;
import io.netty.handler.codec.http.DefaultFullHttpRequest;
import io.netty.handler.codec.http.DefaultHttpHeaders;
import io.netty.handler.codec.http.DefaultHttpRequest;
import io.netty.handler.codec.http.FullHttpResponse;
import io.netty.handler.codec.http.HttpHeaders;
import io.netty.handler.codec.http.HttpMethod;
import io.netty.handler.codec.http.HttpObject;
import io.netty.handler.codec.http.HttpRequest;
import io.netty.handler.codec.http.HttpVersion;
import io.netty.handler.codec.http.multipart.DiskAttribute;
import io.netty.handler.codec.http.multipart.DiskFileUpload;
import io.netty.handler.codec.http.multipart.HttpDataFactory;
import io.netty.handler.codec.http.multipart.HttpPostRequestEncoder;
import io.netty.util.CharsetUtil;

import java.io.File;
import java.net.InetSocketAddress;
import java.util.Map.Entry;
import java.util.logging.Level;
import java.util.logging.Logger;

import reservoir.example.util.STAGE;

public class StreamFileServerHandler extends SimpleChannelInboundHandler<HttpObject> {

    private static final Logger logger = Logger.getLogger(StreamFileServerHandler.class.getName());
    static final String REQ_READY = "/ready";
    static final String REQ_UPLOADFILE = "/uploadFile";
    static int clientsNum;
    HttpDataFactory factory;

    private String uploadFile = "/tmp/nettyTest/server/source.txt";

    static {
        DiskFileUpload.deleteOnExitTemporaryFile = true;
        DiskFileUpload.baseDirectory = null;
        DiskAttribute.deleteOnExitTemporaryFile = true;
        DiskAttribute.baseDirectory = null;

    }
    private Channel channel;
    private boolean readingChunks;
    private HttpHeaders headers;
    private STAGE stage;

    public StreamFileServerHandler(HttpDataFactory _factory) {
        factory = _factory;
    }

    @Override
    public void channelActive(final ChannelHandlerContext ctx) {
        clientsNum++;
        channel = ctx.channel();
        InetSocketAddress remoteAddress = ((InetSocketAddress) ctx.channel().remoteAddress());
        initHeaders(remoteAddress.getHostString(), remoteAddress.getPort());
        logger.log(Level.INFO, remoteAddress.getHostString() + "/" + remoteAddress.getPort());
        try {
            // request /ready
            requestReadyCommand();
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    @Override
    public void channelRead0(ChannelHandlerContext ctx, HttpObject msg) throws Exception {
        if (msg instanceof FullHttpResponse) {
            FullHttpResponse response = (FullHttpResponse) msg;

            readingChunks = HttpHeaders.isTransferEncodingChunked(response);

            System.out.println(response.content().toString(CharsetUtil.UTF_8));
            String referer = response.headers().get("Refer-Command");
            System.out.println("referer = " + referer);
            if (referer == null) {
                System.err.println("The refer-command is null");
            } else if (referer.equalsIgnoreCase(STAGE.READY.name())) {
                stage = STAGE.READY;
                postFile(new File(uploadFile));
            } else if (referer.equalsIgnoreCase(STAGE.UPLOAD_FILE.name())) {
                stage = STAGE.UPLOAD_FILE;
                // postFile(new File(uploadFile));
                ctx.channel().close();
            }
            return;
        }
        // if (msg instanceof HttpContent) {
        // System.out.println("error");
        // HttpContent chunk = (HttpContent) msg;
        //
        // if (chunk instanceof LastHttpContent) {
        // if (readingChunks) {
        // System.out.println("Chunked");
        // } else {
        // System.out.println("UnChunked");
        // }
        // switch (stage) {
        // case READY:
        // postFile(new File(uploadFile));
        // break;
        // case UPLOAD_FILE:
        // System.out.println("END");
        // ctx.channel().close();
        // break;
        // default:
        // System.out.println("Unknown operation");
        // break;
        // }
        // readingChunks = false;
        // } else {
        // System.out.println(chunk.content().toString(CharsetUtil.UTF_8));
        // }
        // }
    }

    private void requestReadyCommand() throws Exception {
        HttpRequest request = new DefaultFullHttpRequest(HttpVersion.HTTP_1_1, HttpMethod.POST,
                REQ_READY);
        for (Entry<String, String> entry : headers) {
            request.headers().set(entry.getKey(), entry.getValue());
        }
        request.headers().set(HttpHeaders.Names.CONTENT_LENGTH, 0);
        channel.write(request);
        channel.flush();
    }

    private void postFile(File file) throws Exception {
        HttpRequest request = new DefaultHttpRequest(HttpVersion.HTTP_1_1, HttpMethod.POST,
                REQ_UPLOADFILE);
        for (Entry<String, String> entry : headers) {
            request.headers().set(entry.getKey(), entry.getValue());
        }
        HttpPostRequestEncoder bodyRequestEncoder = new HttpPostRequestEncoder(factory, request,
                true);

        // bodyRequestEncoder.addBodyFileUpload("myfile", file,
        // "application/x-zip-compressed", false);
        bodyRequestEncoder.addBodyFileUpload("myfile", file, "text/xml", false);

        request = bodyRequestEncoder.finalizeRequest();
        channel.write(request);
        if (bodyRequestEncoder.isChunked()) {
            channel.write(bodyRequestEncoder);
        }
        channel.flush();
        // Now no more use of file representation (and list of HttpData)
        bodyRequestEncoder.cleanFiles();
    }

    @Override
    public void exceptionCaught(ChannelHandlerContext ctx, Throwable cause) {
        logger.log(Level.WARNING, "exceptionCaught", cause);
        cause.printStackTrace();
        ctx.channel().close();
    }

    private HttpHeaders initHeaders(String host, int port) {
        if (headers == null) {
            headers = new DefaultHttpHeaders();
            headers.set(HttpHeaders.Names.CONNECTION, HttpHeaders.Values.KEEP_ALIVE);
            // headers.set(HttpHeaders.Names.ACCEPT_ENCODING, HttpHeaders.Values.GZIP +
            // ','
            // + HttpHeaders.Values.DEFLATE);
            //
            // headers.set(HttpHeaders.Names.ACCEPT_CHARSET,
            // "ISO-8859-1,utf-8;q=0.7,*;q=0.7");
            // headers.set(HttpHeaders.Names.ACCEPT_LANGUAGE, "us");
            // headers.set(HttpHeaders.Names.USER_AGENT, "Netty Simple Http Client side");
            // headers.set(HttpHeaders.Names.ACCEPT,
            // "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8");

            // connection will not close but needed
            // headers.set("Connection","keep-alive");
            // headers.set("Keep-Alive","300");

        }
        return headers;
    }
}
