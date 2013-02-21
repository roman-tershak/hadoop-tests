package hadoop.hdfs.tests;

import java.net.URI;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.AbstractFileSystem;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.Hdfs;
import org.apache.hadoop.fs.Path;

public class Main02 {

    public static void main(String[] args) throws Exception {
        Configuration conf = new Configuration(true);
        conf.set("fs.default.name", "hdfs://192.168.140.128/");
        URI uri = URI.create("hdfs://192.168.140.128/user/rtershak");
        AbstractFileSystem fs = Hdfs.get(uri, conf);
        
        Path path = new Path(URI.create("hdfs://192.168.140.128/user/rtershak/01.data"));
        FSDataInputStream inputStream = fs.open(path);
        
        int counter = 0;
        while (inputStream.read() != -1) {
            counter++;
            if ((counter % 1000) == 0) {
                System.out.println(counter + " bytes are read");
            }
        }
        System.out.println("The end of the input stream reached. " + counter + " bytes are read in total");
        
        inputStream.close();
    }

}
