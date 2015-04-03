package reservoir.example.client;


import io.netty.channel.ChannelInitializer;
import io.netty.channel.ChannelPipeline;
import io.netty.channel.socket.SocketChannel;
import io.netty.handler.codec.http.HttpRequestDecoder;
import io.netty.handler.codec.http.HttpResponseEncoder;

public class StreamFileReceiverInitializer extends ChannelInitializer<SocketChannel> {
    private int rank;

    public StreamFileReceiverInitializer(int rank) {
        this.rank = rank;
    }

    @Override
    public void initChannel(SocketChannel ch) {
        ChannelPipeline pipeline = ch.pipeline();

        pipeline.addLast(new HttpRequestDecoder());
        pipeline.addLast(new HttpResponseEncoder());

        pipeline.addLast(new StreamFileReceiverHandler(rank));
    }
}