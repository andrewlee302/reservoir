package reservoir.example.util;

import java.util.HashMap;
import java.util.Map;

public class StreamFileMetaUtil {
    private static Map<Long, String[]> metas;
    
    
    static{
        metas = new HashMap<Long, String[]>();
    }
    
    public static String generateFileNameAndStore(long counter, long time, int rank, int size){
        String filename = generateFileName(counter, time, rank);
        String[] fileNames = metas.get(counter);
        if(fileNames == null) {
            fileNames = new String[size];
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
            return fileNames[rank];
        }
    }
}
