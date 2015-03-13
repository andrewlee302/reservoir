package reservoir.example.server;

import io.netty.channel.ChannelInitializer;
import io.netty.channel.ChannelPipeline;
import io.netty.channel.socket.SocketChannel;
import io.netty.handler.codec.http.HttpRequestDecoder;
import io.netty.handler.codec.http.HttpResponseEncoder;
import io.netty.handler.ssl.SslContext;

public class HelloWorldServerInitializer extends ChannelInitializer<SocketChannel> {

    private final SslContext sslCtx;

    public HelloWorldServerInitializer(SslContext sslCtx) {
        this.sslCtx = sslCtx;
    }


    @Override
    public void initChannel(SocketChannel ch) {
        ChannelPipeline p = ch.pipeline();
        if (sslCtx != null) {
            p.addLast(sslCtx.newHandler(ch.alloc()));
        }
        p.addLast(new HttpRequestDecoder());
        p.addLast(new HttpResponseEncoder());
//        p.addLast("aggregator", new HttpObjectAggregator(HttpObjectAggregator.DEFAULT_MAX_COMPOSITEBUFFER_COMPONENTS));
        p.addLast(new HelloWorldServerHandler());
    }
}
