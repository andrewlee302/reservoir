package reservoir.example.server;

import io.netty.bootstrap.ServerBootstrap;
import io.netty.channel.Channel;
import io.netty.channel.EventLoopGroup;
import io.netty.channel.nio.NioEventLoopGroup;
import io.netty.channel.socket.nio.NioServerSocketChannel;
import io.netty.handler.codec.http.multipart.DefaultHttpDataFactory;
import io.netty.handler.codec.http.multipart.HttpDataFactory;
import io.netty.handler.logging.LogLevel;
import io.netty.handler.logging.LoggingHandler;


public class StreamFileServer {
    static final int INTERNAL_PORT = 8080;
    static final int APP_PORT = 8081;
    static final String BASE_URL = System.getProperty("baseUrl", "http://127.0.0.1:8080/");

    // static final String FILE = System.getProperty("file", "upload.txt");

    public static void main(String[] args) throws Exception {
        // File file = new File(FILE);
        // if (!file.canRead()) {
        // throw new FileNotFoundException(FILE);
        // }

        // Configure the server.
        EventLoopGroup bossGroup = new NioEventLoopGroup(2);
        EventLoopGroup workerGroup = new NioEventLoopGroup();

        // setup the factory: here using a mixed memory/disk based on size
        // threshold
        HttpDataFactory factory = new DefaultHttpDataFactory(DefaultHttpDataFactory.MINSIZE); 

        try {
            ServerBootstrap internalServer = new ServerBootstrap();
            internalServer.group(bossGroup, workerGroup).channel(NioServerSocketChannel.class)
                    .handler(new LoggingHandler(LogLevel.INFO))
                    .childHandler(new StreamFileServerInitializer(factory));
            Channel internalCh = internalServer.bind(INTERNAL_PORT).sync().channel();
            
            ServerBootstrap appServer = new ServerBootstrap();
            appServer.group(bossGroup, workerGroup).channel(NioServerSocketChannel.class)
                    .handler(new LoggingHandler(LogLevel.INFO))
                    .childHandler(new StreamSourceInitializer());
            Channel appCh = appServer.bind(APP_PORT).sync().channel();
            
            appCh.closeFuture().sync();
            internalCh.closeFuture().sync();
            
        } finally {
            
            // Shut down executor threads to exit.
            bossGroup.shutdownGracefully();
            workerGroup.shutdownGracefully();

            // Really clean all temporary files if they still exist
            factory.cleanAllHttpDatas();
        }

    }
}
