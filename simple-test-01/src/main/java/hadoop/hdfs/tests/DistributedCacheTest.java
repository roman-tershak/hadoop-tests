package hadoop.hdfs.tests;

import java.io.IOException;
import java.net.URI;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.filecache.DistributedCache;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;

public class DistributedCacheTest {

    
    public static void main(String[] args) throws IOException {
        testCreateDistributedFile();
    }

    private static void testCreateDistributedFile() throws IOException {
        URI uri = URI.create("hdfs://192.168.140.129/user/rtershak/data/df-01.data");
        Path path = new Path(uri);
        Configuration conf = new Configuration();
        
        FileSystem fs = FileSystem.get(uri, conf);

        FSDataOutputStream outputStream = fs.create(path, (short) 1);
        
        outputStream.write("Hello distributed world!".getBytes());
        outputStream.close();
        
        DistributedCache.addFileToClassPath(path, conf, fs);
    }
}
