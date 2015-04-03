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
import java.net.URI;
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

            String referer = response.headers().get("Refer-Command");
            if (referer == null) {
                System.err.println("The refer-command is null");
            } else if (referer.equalsIgnoreCase(STAGE.RANK.name())) {
                // stage = STAGE.RANK;
                rank = Integer.parseInt(response.content().toString(CharsetUtil.UTF_8));
                logger.info("Get the #rank:" + rank);
                checkReady();
                postFile(ctx, new File(new URI(streamFileServerdir + StreamFileMetaUtil.getFileNameByCounterAndRank(counter, rank))));
            } else if (referer.equalsIgnoreCase(STAGE.UPLOAD_FILE.name())) {
                // stage = STAGE.UPLOAD_FILE;
                checkReady();
                postFile(ctx, new File(new URI(streamFileServerdir + StreamFileMetaUtil.getFileNameByCounterAndRank(counter, rank))));
            }
            return;
        }
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

    private void checkReady() throws Exception {
        logger.info("Request for ready signal");
        logger.info("---->Check whether the next counter file(counter = "+ (counter + 1) +") is finished writing");
        
        while(!StreamFileMetaUtil.canSafelyTransfer(counter+1)){
            // TODO sleep param, based on batch interval
            Thread.sleep(100);
        }
        counter++;
        logger.info("---->Counter = " + counter  + " file is finished");
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
        }
        return headers;
    }
}
