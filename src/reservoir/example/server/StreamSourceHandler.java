package reservoir.example.server;

import io.netty.buffer.ByteBuf;
import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.SimpleChannelInboundHandler;

import java.io.BufferedOutputStream;
import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.util.logging.Level;
import java.util.logging.Logger;

import reservoir.example.util.StreamFileMetaUtil;

public class StreamSourceHandler extends SimpleChannelInboundHandler<Object> {

    private static final Logger logger = Logger.getLogger(StreamSourceHandler.class.getName());
    private long  counter;
    private long lineCounter;
    private long startTime, batchStartTime;
    private long batchInterval;
    private int clientsNum = 2;
    
    private int lineNumForSplitParam = 100;
    private String streamFileDirParam = "/tmp/nettyTest/server/cached/";
    
    private BufferedOutputStream[] writers;
    
    /**
     * 
     * @param _batchInterval batch interval in milliseconds
     */
    public StreamSourceHandler(long _batchInterval){
        batchInterval = _batchInterval;
    }
    
    @Override
    public void channelActive(final ChannelHandlerContext ctx) {
        
        // read the #clients from the DataMPI master and StreamFileServer, compare them.
        // if not equal, then report the exception
        // TODO
        writers = new BufferedOutputStream[clientsNum];
        System.out.println("Connecting the APP!");
        counter = 1;
        lineCounter = 1;
        batchStartTime = startTime = System.currentTimeMillis();
        try {
            initWriters(writers);
        } catch (IOException e) {
            logger.log(Level.SEVERE, "Failed in initializing the file handler", e);
        }
    }

    @Override
    public void channelRead0(ChannelHandlerContext ctx, Object msg) {
        long now = System.currentTimeMillis();
        lineCounter += 1;
//        System.out.println("lineCounter : " + lineCounter);
        if(now - batchStartTime < batchInterval){
            writeToLocalFile(ctx, msg, counter, counter);
        } else {
            long oldCounter = counter++;
            batchStartTime = now;
            lineCounter = 1;
            writeToLocalFile(ctx, msg, oldCounter, counter);
        }
    }
    
    private void initWriters(BufferedOutputStream[] writers) throws IOException{
        System.out.println("Initial writers");
        for(int i = 0; i < writers.length; i++){
            File file = new File(streamFileDirParam + StreamFileMetaUtil.generateFileNameAndStore(counter, batchStartTime, i, clientsNum));
            if(!file.exists()){
                if(!file.createNewFile())
                    throw new IOException("Can't create the file:" + file.getPath());
            }
            writers[i] = new BufferedOutputStream(new FileOutputStream(file));
        }
    }
    
    private void writeToLocalFile(ChannelHandlerContext ctx, Object msg, long oldCounter, long newCounter) {
        if(!(newCounter == oldCounter + 1) && !(newCounter == oldCounter)){
            logger.log(Level.SEVERE, "Old counter and new counter is not normal");
        }
        if(newCounter == oldCounter + 1) {
            try {
                initWriters(writers);
            } catch (IOException e) {
                logger.log(Level.SEVERE, "Failed in initializing the file handler", e);
                return;
            }
        }
        BufferedOutputStream w = writers[(int) (lineCounter / lineNumForSplitParam) % clientsNum];
        try {
            ByteBuf in = (ByteBuf) msg;
            System.out.println("\nreceive  counter:" + newCounter);
            while (in.isReadable()) {
                byte b = in.readByte();
                w.write(b);
            }
            w.flush();
        } catch (IOException e) {
            e.printStackTrace();
        }
    }
    
    @Override
    public void channelUnregistered(ChannelHandlerContext ctx)
            throws Exception{
        try {
            if(writers != null) {
                for(BufferedOutputStream w: writers){
                    w.flush();
                    w.close();
                    w = null;
                }
            }
        } catch (IOException e) {
            writers = null;
            e.printStackTrace();
        }
        ctx.channel().close().sync();
        long endTime = System.currentTimeMillis();
        long duringTime = endTime - startTime;
        System.out.println("\nEnd. Total time : " + duringTime/1000 + "s");
    }
    
    @Override
    public void exceptionCaught(ChannelHandlerContext ctx, Throwable cause) {
        logger.log(Level.WARNING, "exceptionCaught", cause);
        cause.printStackTrace();
        try {
            if(writers != null) {
                for(BufferedOutputStream w: writers){
                    w.close();
                    w = null;
                }
            }
        } catch (IOException e) {
            writers = null;
            e.printStackTrace();
        }
        ctx.channel().close();
    }
}