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
import io.netty.handler.codec.http.HttpHeaders;
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
import java.util.Random;

import reservoir.example.util.STAGE;

public class StreamFileReceiverHandler extends SimpleChannelInboundHandler<HttpObject> {

    private boolean readingChunks;
    private STAGE stage;

    private final StringBuilder responseContent = new StringBuilder();

    private static final HttpDataFactory factory = new DefaultHttpDataFactory(
            DefaultHttpDataFactory.MINSIZE);
    private HttpPostRequestDecoder decoder;
    private String destDir = "/tmp/nettyTest/client/";

    static {
        DiskFileUpload.deleteOnExitTemporaryFile = true;
        DiskFileUpload.baseDirectory = "/tmp/nettyTest/client/tmp/";
        DiskAttribute.deleteOnExitTemporaryFile = true;
        DiskAttribute.baseDirectory = null;
    }

    @Override
    public void channelRead0(ChannelHandlerContext ctx, HttpObject msg) throws Exception {
        if (msg instanceof HttpRequest) {
            HttpRequest request = (HttpRequest) msg;
            readingChunks = HttpHeaders.isTransferEncodingChunked(request);
            if (request.getMethod().equals(HttpMethod.POST)) {
                decoder = new HttpPostRequestDecoder(factory, request);
            } else if (request.getMethod().equals(HttpMethod.GET)) {
                responseContent.append("get method");
                writeResponse(ctx);
                return;
            }

            URI uri = new URI(request.getUri());
            System.out.println(request.getUri());
            if (uri.getPath().startsWith("/ready")) {
                stage = STAGE.READY;
            } else if (uri.getPath().startsWith("/uploadFile")) {
                stage = STAGE.UPLOAD_FILE;
                System.out.println("Start to receive the file");
            } else {
                System.out.println("ERROR");
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
                    case READY:
                        System.out.println("Register succeed");
                        responseContent.append("READY_OK");
                        break;
                    case UPLOAD_FILE:
                        System.out.println("UPLOAD_SUCCESS");
                        responseContent.append("UPLOAD_SUCCESS");
                        break;
                    default:
                        System.out.println("Unknown operation");
                        responseContent.append("Unknown operation");
                        reset();
                        return;
                    }
                    writeResponse(ctx);
                    readingChunks = false;
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
        } catch (EndOfDataDecoderException e1) {
        }
    }

    private void writeHttpData(InterfaceHttpData data) throws IOException {
        if (data.getHttpDataType() == HttpDataType.Attribute) {
            Attribute attr = (Attribute) data;
            System.out.println(attr);
        } else if (data.getHttpDataType() == HttpDataType.FileUpload) {
            FileUpload fileUpload = (FileUpload) data;
            String filename = fileUpload.getFilename();
            if (fileUpload.isCompleted()) {
                if (fileUpload.length() < 10000) {
                    System.out.println("File length is less than 10000");
                } else {
                    System.out.println("File length is more than 10000");
                }
                Random r = new Random();
                StringBuffer fileNameBuf = new StringBuffer();
                fileNameBuf.append(destDir).append("U").append(System.currentTimeMillis());
                fileNameBuf.append(String.valueOf(r.nextInt(10))).append(
                        String.valueOf(r.nextInt(10)));
                fileNameBuf.append(filename.substring(filename.lastIndexOf(".")));

                fileUpload.renameTo(new File(fileNameBuf.toString()));
                System.out.println(filename + "isInMemroy:" + fileUpload.isInMemory());
                decoder.removeHttpDataFromClean(fileUpload);
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