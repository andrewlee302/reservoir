package reservoir.example.source;

import io.netty.bootstrap.Bootstrap;
import io.netty.channel.Channel;
import io.netty.channel.ChannelFuture;
import io.netty.channel.ChannelFutureListener;
import io.netty.channel.DefaultFileRegion;
import io.netty.channel.EventLoopGroup;
import io.netty.channel.nio.NioEventLoopGroup;
import io.netty.channel.socket.nio.NioSocketChannel;
import io.netty.handler.codec.http.multipart.DiskAttribute;
import io.netty.handler.codec.http.multipart.DiskFileUpload;

import java.io.RandomAccessFile;

public class FileToSocketClient {
    static String HOST = "127.0.0.1";
    static int PORT = 8081;
    static String FILEPATH = "/tmp/nettyTest/source/source.txt";

    static {
        DiskFileUpload.deleteOnExitTemporaryFile = true;
        DiskFileUpload.baseDirectory = "/tmp/nettyTest/source/tmp/";
        DiskAttribute.deleteOnExitTemporaryFile = true;
        DiskAttribute.baseDirectory = null;
    }

    public static void main(String[] args) throws Exception {
        if (args.length > 0 && args[0].equals("exp")) {
            if (args.length != 4) {
                System.out.println("param: exp HOST PORT FILEPATH");
                System.exit(1);
            } else {
                HOST = args[1];
                PORT = Integer.parseInt(args[2]);
                FILEPATH = args[3];
            }
        }
        EventLoopGroup group = new NioEventLoopGroup();
//        HttpDataFactory factory = new DefaultHttpDataFactory(DefaultHttpDataFactory.MINSIZE);
        Channel channel = null;
        try {
            Bootstrap b = new Bootstrap();
            b.group(group).channel(NioSocketChannel.class).handler(new FileToSocketInitializer());

            channel = b.connect(HOST, PORT).sync().channel();
//            for (int i = 0; i < 100; i++) {
            while(true){
                final long start = System.currentTimeMillis();
                // HttpRequest request = new DefaultHttpRequest(HttpVersion.HTTP_1_1,
                // HttpMethod.POST,
                // "/");
                // HttpHeaders headers = request.headers();
                //
                // headers.set(HttpHeaders.Names.CONNECTION,
                // HttpHeaders.Values.KEEP_ALIVE);
                //
                // HttpPostRequestEncoder bodyRequestEncoder = new
                // HttpPostRequestEncoder(factory,
                // request, true);
                // File myFile = new File(FILEPATH);
                // bodyRequestEncoder.addBodyFileUpload("myfile", myFile, "text/xml",
                // false);
                // // System.out.println(myFile.length()/1024.0/1024 + "M");
                // request = bodyRequestEncoder.finalizeRequest();
                //
                // channel.write(request);
                // if (bodyRequestEncoder.isChunked()) {
                // channel.write(bodyRequestEncoder).addListener(new
                // ChannelFutureListener() {
                // @Override
                // public void operationComplete(ChannelFuture future) {
                // long end = System.currentTimeMillis();
                // System.out.println("Total transfer time : " + (end - start) / 1000
                // + "s");
                // // channel.close();
                // }
                // });
                // }
                // channel.flush();
                // bodyRequestEncoder.cleanFiles();

                RandomAccessFile raf = null;
                long length = -1;
                try {
                    raf = new RandomAccessFile(FILEPATH, "r");
                    length = raf.length();
                } catch (Exception e) {
                    System.out.println("ERR: " + e.getClass().getSimpleName() + ": "
                            + e.getMessage() + '\n');
                    return;
                } finally {
                    if (length < 0 && raf != null) {
                        raf.close();
                    }
                }
                channel.write(new DefaultFileRegion(raf.getChannel(), 0, length)).addListener(
                        new ChannelFutureListener() {
                            @Override
                            public void operationComplete(ChannelFuture future) {
                                long end = System.currentTimeMillis();
                                System.out.println("Total transfer time : " + (end - start) / 1000
                                        + "s");
                            }
                        });
                channel.flush();
                Thread.sleep(1000);
            }
        } finally {
            if(channel != null)
                channel.closeFuture().sync();
            group.shutdownGracefully();
        }
    }
}
