package reservoir.example.server;

import io.netty.channel.ChannelInitializer;
import io.netty.channel.ChannelPipeline;
import io.netty.channel.socket.SocketChannel;
import io.netty.handler.codec.http.HttpClientCodec;
import io.netty.handler.codec.http.HttpObjectAggregator;
import io.netty.handler.codec.http.multipart.HttpDataFactory;
import io.netty.handler.stream.ChunkedWriteHandler;

public class StreamFileServerInitializer extends ChannelInitializer<SocketChannel> {
    HttpDataFactory factory;

    public StreamFileServerInitializer(HttpDataFactory _factory) {
        factory = _factory;
    }

    @Override
    public void initChannel(SocketChannel ch) {
        ChannelPipeline pipeline = ch.pipeline();

        pipeline.addLast("codec", new HttpClientCodec());

        // to be used since huge file transfer
        pipeline.addLast("chunkedWriter", new ChunkedWriteHandler());

        pipeline.addLast("aggreator",new HttpObjectAggregator(HttpObjectAggregator.DEFAULT_MAX_COMPOSITEBUFFER_COMPONENTS));
        
        pipeline.addLast("handler", new StreamFileServerHandler(factory));
        
    }
}
