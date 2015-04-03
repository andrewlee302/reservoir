package reservoir.example.client;

import static io.netty.buffer.Unpooled.copiedBuffer;
import static io.netty.handler.codec.http.HttpHeaders.Names.CONNECTION;
import static io.netty.handler.codec.http.HttpHeaders.Names.CONTENT_LENGTH;
import static io.netty.handler.codec.http.HttpHeaders.Names.CONTENT_TYPE;
import io.netty.buffer.ByteBuf;
import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.SimpleChannelInboundHandler;
import io.netty.handler.codec.http.DefaultFullHttpResponse;
import io.netty.handler.codec.http.FullHttpResponse;
import io.netty.handler.codec.http.HttpContent;
import io.netty.handler.codec.http.HttpHeaders.Values;
import io.netty.handler.codec.http.HttpMethod;
import io.netty.handler.codec.http.HttpObject;
import io.netty.handler.codec.http.HttpRequest;
import io.netty.handler.codec.http.HttpResponseStatus;
import io.netty.handler.codec.http.HttpVersion;
import io.netty.handler.codec.http.LastHttpContent;
import io.netty.handler.codec.http.multipart.Attribute;
import io.netty.handler.codec.http.multipart.DefaultHttpDataFactory;
import io.netty.handler.codec.http.multipart.DiskAttribute;
import io.netty.handler.codec.http.multipart.DiskFileUpload;
import io.netty.handler.codec.http.multipart.FileUpload;
import io.netty.handler.codec.http.multipart.HttpDataFactory;
import io.netty.handler.codec.http.multipart.HttpPostRequestDecoder;
import io.netty.handler.codec.http.multipart.HttpPostRequestDecoder.EndOfDataDecoderException;
import io.netty.handler.codec.http.multipart.HttpPostRequestDecoder.ErrorDataDecoderException;
import io.netty.handler.codec.http.multipart.InterfaceHttpData;
import io.netty.handler.codec.http.multipart.InterfaceHttpData.HttpDataType;
import io.netty.util.CharsetUtil;

import java.io.File;
import java.io.IOException;
import java.net.URI;
import java.net.URISyntaxException;
import java.util.logging.Logger;

import reservoir.example.util.STAGE;
import reservoir.example.util.StreamFileMetaUtil;


// TODO Whether it need synchronization when transfer the files in multi-channels. 

public class StreamFileReceiverHandler extends SimpleChannelInboundHandler<HttpObject> {

    private static final Logger logger = Logger.getLogger(StreamFileReceiverHandler.class.getName());
    
    // private static AtomicInteger rankCounter = new AtomicInteger(0);
    
    private STAGE stage;
    private long counter;

    private final StringBuilder responseContent = new StringBuilder();

    private static final HttpDataFactory factory = new DefaultHttpDataFactory(
            DefaultHttpDataFactory.MINSIZE);
    private HttpPostRequestDecoder decoder;
    
    private int rank = 0;
    private String destDir;

    static {
        DiskFileUpload.deleteOnExitTemporaryFile = true;
        DiskFileUpload.baseDirectory = null;
        DiskAttribute.deleteOnExitTemporaryFile = true;
        DiskAttribute.baseDirectory = null;
    }

    public StreamFileReceiverHandler(int rank){
        this.rank = rank;
        destDir = StreamFileMetaUtil.getStreamFileClientDirParam();
    }
    
    @Override
    public void channelRead0(ChannelHandlerContext ctx, HttpObject msg) throws Exception {
        if (msg instanceof HttpRequest) {
            HttpRequest request = (HttpRequest) msg;
            if (request.getMethod().equals(HttpMethod.POST)) {
                decoder = new HttpPostRequestDecoder(factory, request);
            } else if (request.getMethod().equals(HttpMethod.GET)) {
                responseContent.append("get method");
                writeResponse(ctx);
                return;
            }

            URI uri = new URI(request.getUri());
            System.out.println(request.getUri());
            if (uri.getPath().startsWith("/rank")) {
                logger.info("Get the RANK request");
                stage = STAGE.RANK;
            } else if (uri.getPath().startsWith("/uploadFile")) {
                logger.info("Get the UPLOAD_FILE request");
                logger.info("Start to receive the file");
                stage = STAGE.UPLOAD_FILE;
            } else {
                logger.warning("Invalid request");
            }
        }
        if (decoder != null) {
            if (msg instanceof HttpContent) {
                HttpContent chunk = (HttpContent) msg;
                try {
                    decoder.offer(chunk);
                } catch (ErrorDataDecoderException e1) {
                    e1.printStackTrace();
                    writeResponse(ctx);
                    return;
                }
                readHttpDataChunkByChunk();

                if (chunk instanceof LastHttpContent) {
                    switch (stage) {
                    case RANK:
                        responseContent.append(rank);
                        break;
                    case UPLOAD_FILE:
                        responseContent.append("UPLOAD_SUCCESS");
                        break;
                    default:
                        reset();
                        return;
                    }
                    writeResponse(ctx);
                    reset();
                }
            }
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

    void readHttpDataChunkByChunk() throws IOException {
        try {
            while (decoder.hasNext()) {
                InterfaceHttpData data = decoder.next();
                if (data != null) {
                    try {
                        writeHttpData(data);
                    } finally {
                        data.release();
                    }
                }
            }
        } catch (EndOfDataDecoderException e) {
            e.printStackTrace();
        }
    }

    private void writeHttpData(InterfaceHttpData data) throws IOException {
        if (data.getHttpDataType() == HttpDataType.Attribute) {
            Attribute attr = (Attribute) data;
            if(attr.getName().equals("counter")){
                counter = Long.parseLong(attr.getValue());
                logger.info("--->Get the counter from the request:" + counter);
            } else {
                logger.info("--->" + attr);
            }
        } else if (data.getHttpDataType() == HttpDataType.FileUpload) {
            FileUpload fileUpload = (FileUpload) data;
            String uploadFileName = fileUpload.getFilename();
            logger.info("--->Receiving file's name: " + uploadFileName);
            if (fileUpload.isCompleted()) {
                if (fileUpload.length() < 10000) {
                    logger.info("File length is less than 10000");
                } else {
                    logger.info("File length is more than 10000");
                }
                String storeFileName = destDir + uploadFileName;
                logger.info("--->Write the file to disk :" + storeFileName);
                long startTime = System.currentTimeMillis();
                File storeFile = null;
                try {
                    storeFile = new File(new URI(storeFileName));
                    if(!storeFile.getParentFile().exists())
                        storeFile.getParentFile().mkdirs();
                    if(fileUpload.renameTo(storeFile)){
                        logger.info("--->Write completed");
                        logger.info("--->Time taken :" + (System.currentTimeMillis() - startTime)/1000.0 + "s");;
                    }else
                        System.out.println("Write failed");
                     decoder.removeHttpDataFromClean(fileUpload);
                } catch (URISyntaxException e) {
                    e.printStackTrace();
                }
                
            }
        }
    }

    private void writeResponse(ChannelHandlerContext ctx) {
        // Convert the response content to a ChannelBuffer.
        ByteBuf buf = copiedBuffer(responseContent.toString(), CharsetUtil.UTF_8);
        responseContent.setLength(0);

        FullHttpResponse response = new DefaultFullHttpResponse(HttpVersion.HTTP_1_1,
                HttpResponseStatus.OK, buf);
        response.headers().set(CONTENT_TYPE, "text/plain; charset=UTF-8");
        response.headers().set("Refer-Command", stage);
        response.headers().set(CONTENT_LENGTH, buf.readableBytes());
        response.headers().set(CONNECTION, Values.KEEP_ALIVE);
        ctx.writeAndFlush(response);
        stage = null;
    }

    @Override
    public void exceptionCaught(ChannelHandlerContext ctx, Throwable cause) throws Exception {
        ctx.channel().close();
    }
}