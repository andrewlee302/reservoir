package reservoir.example.util;

import java.util.HashMap;
import java.util.Map;

public class StreamFileMetaUtil {
    public static final long INITIAL_COUNTER = 1;

    private static Map<Long, String[]> metas;
    
    private static long counter = INITIAL_COUNTER - 1; // the number of batch which is being writing
    
    private static long lastestFinishedTransferCounter = INITIAL_COUNTER - 1;
    
    
    // TODO scheme is important, local file don 
    private static String streamFileServerDirParam = "file:///tmp/nettyTest/server/cached/";
    
    private static String streamFileClientDirParam = "file:///tmp/nettyTest/client/cached/";
    
    public static void setStreamFileServerDirParam(String dir) {
        streamFileServerDirParam = dir;
    }
    
    public static void setStreamFileClientDirParam(String dir) {
        streamFileClientDirParam = dir;
    }
    
    public static String getStreamFileServerDirParam(){ return streamFileServerDirParam; }
    public static String getStreamFileClientDirParam(){ return streamFileClientDirParam; }
    
    
    static{
        metas = new HashMap<Long, String[]>();
    }
    
    /**
     * Whether the function need synchronized? Now is just called by one thread.
     * @param counter
     * @param time
     * @param rank
     * @param size
     * @return the absolute path of the file
     */
    public static String generateFileNameAndStore(long counter, long time, int rank, int size){
        StreamFileMetaUtil.counter = counter;
        String filename = generateFileName(counter, time, rank);
        String[] fileNames = metas.get(counter);
        if(fileNames == null) {
            fileNames = new String[size];
            metas.put(counter, fileNames);
        }
        fileNames[rank] = filename;
        return filename;
    }
    
    private static String generateFileName(long counter, long time, int rank){
        return counter + "-" + time + "-" + rank;
    }
    
    public static String getFileNameByCounterAndRank(long counter, int rank){
        String[] fileNames = metas.get(counter);
        if(fileNames == null){
            return null;
        } else {
            if(fileNames[rank] != null) 
                return fileNames[rank];
            else
                return null;
        }
    }
    
    /**
     * 
     * @param requestTransferCounter The request transferring counter of batch at the server dispatcher
     * @return
     */
    public static boolean canSafelyTransfer(long requestTransferCounter){
        lastestFinishedTransferCounter = requestTransferCounter - 1;
        if(requestTransferCounter <= counter - 1)
            return true;
        else
            return false;
    }
    
    public static long getLastestFinishedTransferCounter() {
        return lastestFinishedTransferCounter;
    }
}
