package reservoir.example.server;

import io.netty.buffer.ByteBuf;
import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.SimpleChannelInboundHandler;

import java.io.BufferedOutputStream;
import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.net.URI;
import java.net.URISyntaxException;
import java.util.logging.Level;
import java.util.logging.Logger;

import reservoir.example.util.StreamFileMetaUtil;

public class StreamSourceHandler extends SimpleChannelInboundHandler<Object> {

    private static final Logger logger = Logger.getLogger(StreamSourceHandler.class.getName());
    private long counter;
    private long lineCounter;
    private long startTime, batchStartTime;
    private long batchInterval;
    private int clientsNum;
    private long lineNumForSplitParam;
    
    private BufferedOutputStream[] writers;
    
    private final String streamFileServerdir = StreamFileMetaUtil.getStreamFileServerDirParam();
    
    /**
     * 
     * @param _batchInterval batch interval in milliseconds
     * @param _clientsNum
     * @param _lineNumForSplitParam
     */
    public StreamSourceHandler(long _batchInterval, int _clientsNum, long _lineNumForSplitParam){
        batchInterval = _batchInterval;
        clientsNum = _clientsNum;
        lineNumForSplitParam = _lineNumForSplitParam;
    }
    
    /**
     * 
     * @param _batchInterval
     * @param _clientsNum
     * Default value of lineNumForSplitParam is 100
     */
    public StreamSourceHandler(long _batchInterval, int _clientsNum){
        this(_batchInterval, _clientsNum, 100);
    }
    
    @Override
    public void channelActive(final ChannelHandlerContext ctx) {
        
        // read the #clients from the DataMPI master and StreamFileServer, compare them.
        // if not equal, then report the exception
        // TODO
        writers = new BufferedOutputStream[clientsNum];
        counter = StreamFileMetaUtil.INITIAL_COUNTER;
        lineCounter = 1;
        batchStartTime = startTime = System.currentTimeMillis();
        try {
            logger.info("New counter is producing(counter = " + 1 + ")");
            initWriters(writers);
        } catch (IOException e) {
            logger.log(Level.SEVERE, "Failed in initializing the file handler", e);
        }
    }

    @Override
    public void channelRead0(ChannelHandlerContext ctx, Object msg) {
        long now = System.currentTimeMillis();
        lineCounter += 1;
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
        for(int i = 0; i < writers.length; i++){
            if(writers[i] != null){
                writers[i].flush();
                writers[i].close();
                writers[i] = null;
            }
            File file = null;
            try {
                file = new File(new URI(streamFileServerdir + StreamFileMetaUtil.generateFileNameAndStore(counter, batchStartTime, i, clientsNum)));
                if(file != null && !file.exists()){
                    if(!file.createNewFile())
                        throw new IOException("Can't create the file:" + file.getPath());
                }
            } catch (URISyntaxException e) {
                e.printStackTrace();
            }
            writers[i] = new BufferedOutputStream(new FileOutputStream(file));
        }
    }
    
    private void writeToLocalFile(ChannelHandlerContext ctx, Object msg, long oldCounter, long newCounter) {
        if(!(newCounter == oldCounter + 1) && !(newCounter == oldCounter)){
            logger.severe("Old counter and new counter is not normal");
        }
        if(newCounter == oldCounter + 1) {
            logger.info("New counter is producing(counter = " + newCounter+ ")");
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