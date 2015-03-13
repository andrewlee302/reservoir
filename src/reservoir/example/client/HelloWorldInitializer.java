package reservoir.example.client;

import io.netty.channel.ChannelInitializer;
import io.netty.channel.ChannelPipeline;
import io.netty.channel.socket.SocketChannel;
import io.netty.handler.codec.http.HttpClientCodec;
import io.netty.handler.stream.ChunkedWriteHandler;

public class HelloWorldInitializer extends ChannelInitializer<SocketChannel> {

    @Override
    protected void initChannel(SocketChannel ch) throws Exception {
        ChannelPipeline pipe = ch.pipeline();
        pipe.addLast("codec", new HttpClientCodec());
        pipe.addLast("chunkedWriter", new ChunkedWriteHandler());
        pipe.addLast("brower", new HelloWorldClientHandler());
    }
}
