package reservoir.example.source;

import io.netty.channel.ChannelInitializer;
import io.netty.channel.ChannelPipeline;
import io.netty.channel.socket.SocketChannel;
import io.netty.handler.codec.http.HttpClientCodec;
import io.netty.handler.stream.ChunkedWriteHandler;

public class FileToSocketInitializer extends ChannelInitializer<SocketChannel> {

    @Override
    protected void initChannel(SocketChannel ch) throws Exception {
        ChannelPipeline pipe = ch.pipeline();
        pipe.addLast("codec", new HttpClientCodec());
        pipe.addLast("chunkedWriter", new ChunkedWriteHandler());
        pipe.addLast("brower", new FileToScoketHandler());
    }
}
