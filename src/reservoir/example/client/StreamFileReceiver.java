package reservoir.example.client;

import io.netty.bootstrap.Bootstrap;
import io.netty.channel.Channel;
import io.netty.channel.EventLoopGroup;
import io.netty.channel.nio.NioEventLoopGroup;
import io.netty.channel.socket.nio.NioSocketChannel;

/**
 * A HTTP server showing how to use the HTTP multipart package for file uploads and
 * decoding post data.
 */
public final class StreamFileReceiver {
    static final String HOST = "127.0.0.1";
    static final int PORT = 8080;
    static int rankCounter = 0;

    public static void main(String[] args) throws Exception {
        new Thread(new Runnable(){

            @Override
            public void run() {
                EventLoopGroup group = new NioEventLoopGroup();

                try {
                    Bootstrap b = new Bootstrap();
                    b.group(group).channel(NioSocketChannel.class)
                            .handler(new StreamFileReceiverInitializer(rankCounter++));
                    Channel c = b.connect(HOST, PORT).sync().channel();
                    c.closeFuture().sync();
                } catch (InterruptedException e) {
                    e.printStackTrace();
                } finally {
                    group.shutdownGracefully();
                }
            }
            
        }).start();
        new Thread(new Runnable(){

            @Override
            public void run() {
                EventLoopGroup group = new NioEventLoopGroup();

                try {
                    Bootstrap b = new Bootstrap();
                    b.group(group).channel(NioSocketChannel.class)
                            .handler(new StreamFileReceiverInitializer(rankCounter++));
                    Channel c = b.connect(HOST, PORT).sync().channel();
                    c.closeFuture().sync();
                } catch (InterruptedException e) {
                    e.printStackTrace();
                } finally {
                    group.shutdownGracefully();
                }
            }
            
        }).start();
    }
}