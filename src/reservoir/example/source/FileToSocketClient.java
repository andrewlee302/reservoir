package reservoir.example.source;

import io.netty.bootstrap.Bootstrap;
import io.netty.channel.Channel;
import io.netty.channel.ChannelFuture;
import io.netty.channel.ChannelFutureListener;
import io.netty.channel.EventLoopGroup;
import io.netty.channel.nio.NioEventLoopGroup;
import io.netty.channel.socket.nio.NioSocketChannel;
import io.netty.handler.codec.http.DefaultHttpRequest;
import io.netty.handler.codec.http.HttpHeaders;
import io.netty.handler.codec.http.HttpMethod;
import io.netty.handler.codec.http.HttpRequest;
import io.netty.handler.codec.http.HttpVersion;
import io.netty.handler.codec.http.multipart.DefaultHttpDataFactory;
import io.netty.handler.codec.http.multipart.DiskAttribute;
import io.netty.handler.codec.http.multipart.DiskFileUpload;
import io.netty.handler.codec.http.multipart.HttpDataFactory;
import io.netty.handler.codec.http.multipart.HttpPostRequestEncoder;

import java.io.File;

public class FileToSocketClient {
    static String HOST = "127.0.0.1";
    static int PORT = 8081;
    static String FILEPATH = "/tmp/nettyTest/server/source.txt";
    
    static {
        DiskFileUpload.deleteOnExitTemporaryFile = true;
        DiskFileUpload.baseDirectory = "/tmp/nettyTest/source/tmp/";
        DiskAttribute.deleteOnExitTemporaryFile = true;
        DiskAttribute.baseDirectory = null;

    }

    public static void main(String[] args) throws Exception {
        if(args.length > 0 && args[0].equals("exp")) {
            if(args.length != 4){
                System.out.println("param: exp HOST PORT FILEPATH");
                System.exit(1);
            } else {
                HOST = args[1];
                PORT = Integer.parseInt(args[2]);
                FILEPATH = args[3];
            }
        }
        EventLoopGroup group = new NioEventLoopGroup();
        HttpDataFactory factory = new DefaultHttpDataFactory(DefaultHttpDataFactory.MINSIZE); 
        try {
            Bootstrap b = new Bootstrap();
            b.group(group).channel(NioSocketChannel.class)
                    .handler(new FileToSocketInitializer());

             final Channel channel = b.connect(HOST, PORT).sync().channel();
             final long start = System.currentTimeMillis();
            
             HttpRequest request = new DefaultHttpRequest(HttpVersion.HTTP_1_1,
             HttpMethod.POST, "/");
             HttpHeaders headers = request.headers();
            
             headers.set(HttpHeaders.Names.CONNECTION, HttpHeaders.Values.KEEP_ALIVE);

             HttpPostRequestEncoder bodyRequestEncoder = new HttpPostRequestEncoder(factory, request, true); 
//             bodyRequestEncoder.addBodyAttribute("name", "value");
//             bodyRequestEncoder.addBodyAttribute("ccc", "李");
             File myFile = new File(FILEPATH);
             bodyRequestEncoder.addBodyFileUpload("myfile",myFile , "text/xml", false);
             System.out.println(myFile.length()/1024.0/1024 + "M");
             request = bodyRequestEncoder.finalizeRequest();
             
             
             channel.write(request);
             if (bodyRequestEncoder.isChunked()) {
                 channel.write(bodyRequestEncoder).addListener(new ChannelFutureListener() {
                     @Override
                     public void operationComplete(ChannelFuture future) {
                         long end = System.currentTimeMillis();
                         System.out.println("Total transfer time : " + (end - start) / 1000 + "s");
                         channel.close();
                     }
                 });
             }
             channel.flush();
             bodyRequestEncoder.cleanFiles();
             
             
            
//            HttpRequest request = new DefaultFullHttpRequest(HttpVersion.HTTP_1_1, HttpMethod.GET, "/");
//            HttpHeaders headers = request.headers();
//            headers.set(HttpHeaders.Names.CONNECTION, HttpHeaders.Values.KEEP_ALIVE);
//            headers.set(HttpHeaders.Names.CONTENT_LENGTH, 0);
//            channel.writeAndFlush(request);
            channel.closeFuture().sync();
        } finally {
            group.shutdownGracefully();
        }
    }
}
