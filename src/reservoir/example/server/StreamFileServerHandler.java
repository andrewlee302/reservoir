package reservoir.example.server;

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
import java.util.concurrent.atomic.AtomicInteger;
import java.util.logging.Level;
import java.util.logging.Logger;

import reservoir.example.util.STAGE;
import reservoir.example.util.StreamFileMetaUtil;

public class StreamFileServerHandler extends SimpleChannelInboundHandler<HttpObject> {

    
    private static final Logger logger = Logger.getLogger(StreamFileServerHandler.class.getName());
    
    private HttpDataFactory factory;
    private int rank;
    
    private static AtomicInteger clientsNum = new AtomicInteger(0);
    private long counter = StreamFileMetaUtil.INITIAL_COUNTER - 1;
    private final String streamFileServerdir = StreamFileMetaUtil.getStreamFileServerDirParam();
    
    static final String REQ_READY = "/ready";
    static final String REQ_UPLOADFILE = "/uploadFile";
    static final String REQ_RANK = "/rank";
    static {
        DiskFileUpload.deleteOnExitTemporaryFile = true;
        DiskFileUpload.baseDirectory = null;
        DiskAttribute.deleteOnExitTemporaryFile = true;
        DiskAttribute.baseDirectory = null;

    }
    private HttpHeaders headers;
    // private STAGE stage;

    public StreamFileServerHandler(HttpDataFactory _factory) {
        factory = _factory;
    }

    @Override
    public void channelActive(final ChannelHandlerContext ctx) {
        int tmp = clientsNum.incrementAndGet();
        InetSocketAddress remoteAddress = ((InetSocketAddress) ctx.channel().remoteAddress());
        logger.info("Active:" + remoteAddress.getHostString() + "/" + remoteAddress.getPort());
        logger.info("Update ClientNum to " + tmp);
        initHeaders(remoteAddress.getHostString(), remoteAddress.getPort());
        requestRank(ctx);
    }

    @Override
    public void channelRead0(ChannelHandlerContext ctx, HttpObject msg) throws Exception {
        if (msg instanceof FullHttpResponse) {
            FullHttpResponse response = (FullHttpResponse) msg;

            // System.out.println(response.content().toString(CharsetUtil.UTF_8));
            String referer = response.headers().get("Refer-Command");
            // System.out.println("referer = " + referer);
            if (referer == null) {
                System.err.println("The refer-command is null");
            } else if (referer.equalsIgnoreCase(STAGE.RANK.name())) {
                // stage = STAGE.RANK;
                rank = Integer.parseInt(response.content().toString(CharsetUtil.UTF_8));
                logger.info("Get the #rank:" + rank);
                requestReadyCommand(ctx);
            } else if (referer.equalsIgnoreCase(STAGE.READY.name())) {
                // stage = STAGE.READY;
                logger.info("READY");
                postFile(ctx, new File(streamFileServerdir + StreamFileMetaUtil.getFileNameByCounterAndRank(counter, rank)));
            } else if (referer.equalsIgnoreCase(STAGE.UPLOAD_FILE.name())) {
                // stage = STAGE.UPLOAD_FILE;
                requestReadyCommand(ctx);
                // postFile(new File(uploadFile));
                // ctx.channel().close();
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
    
    private void requestRank(ChannelHandlerContext ctx) {
        logger.info("Request for #rank");
        HttpRequest request = new DefaultFullHttpRequest(HttpVersion.HTTP_1_1, HttpMethod.POST,
                REQ_RANK);
        for (Entry<String, String> entry : headers) {
            request.headers().set(entry.getKey(), entry.getValue());
        }
        request.headers().set(HttpHeaders.Names.CONTENT_LENGTH, 0);
        ctx.write(request);
        ctx.flush();
    }

    private void requestReadyCommand(ChannelHandlerContext ctx) throws Exception {
        logger.info("Request for ready signal");
        logger.info("---->Check whether the next counter file(counter = "+ (counter + 1) +") is finished writing");
        long tmpCounter = StreamFileMetaUtil.getLastFinishCounter();
        while(tmpCounter <= counter){
            // TODO sleep param, based on batch interval
            Thread.sleep(100);
            tmpCounter = StreamFileMetaUtil.getLastFinishCounter();
        }
        counter++;
        logger.info("---->Counter = " + counter  + " file is finished");
        HttpRequest request = new DefaultFullHttpRequest(HttpVersion.HTTP_1_1, HttpMethod.POST,
                REQ_READY);
        for (Entry<String, String> entry : headers) {
            request.headers().set(entry.getKey(), entry.getValue());
        }
        HttpPostRequestEncoder bodyRequestEncoder = new HttpPostRequestEncoder(factory, request,
                false);
        bodyRequestEncoder.addBodyAttribute("counter", String.valueOf(counter));
        request = bodyRequestEncoder.finalizeRequest();
        // request.headers().set(HttpHeaders.Names.CONTENT_LENGTH, 0);
        ctx.write(request);
        ctx.flush();
    }

    private void postFile(ChannelHandlerContext ctx, File file) throws Exception {
        logger.info("Start post the file through HTTP protocol");
        if(!file.exists()){
            logger.warning("File " + file.getAbsolutePath() + " doesn't exist");
        }
        HttpRequest request = new DefaultHttpRequest(HttpVersion.HTTP_1_1, HttpMethod.POST,
                REQ_UPLOADFILE);
        for (Entry<String, String> entry : headers) {
            request.headers().set(entry.getKey(), entry.getValue());
        }
        HttpPostRequestEncoder bodyRequestEncoder = new HttpPostRequestEncoder(factory, request,
                true);

        // bodyRequestEncoder.addBodyFileUpload("myfstreamBatchFileile", file, "application/x-zip-compressed", false);
        bodyRequestEncoder.addBodyFileUpload("streamBatchFile", file, "text/xml", false);

        request = bodyRequestEncoder.finalizeRequest();
        ctx.write(request);
        if (bodyRequestEncoder.isChunked()) {
            ctx.write(bodyRequestEncoder);
        }
        ctx.flush();
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
