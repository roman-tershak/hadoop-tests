package hadoop.hdfs.tests;

import java.io.BufferedOutputStream;
import java.io.IOException;
import java.net.URI;
import java.text.NumberFormat;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;

public class ClusterMain01 {

    public static void main(String[] args) throws Exception {
//        testCreation();
//        testCreationWithReplication();
//        testLargeFileCreation();
        testReadingLargeFile();
    }

    private static void testCreation() throws Exception {
        Configuration conf = new Configuration();
        URI uri = URI.create("hdfs://192.168.140.129/user/rtershak/01.data");
        FileSystem fs = FileSystem.get(uri, conf);

        Path path = new Path(uri);
        FSDataOutputStream outputStream = fs.create(path, (short) 1);

        outputStream.write("Hello world!".getBytes());
        outputStream.flush();

        outputStream.close();
    }
    
    private static void testCreationWithReplication() throws Exception {
        Configuration conf = new Configuration();
        URI uri = URI.create("hdfs://192.168.140.129/user/rtershak/01.data");
        FileSystem fs = FileSystem.get(uri, conf);
        
        Path path = new Path(uri);
        FSDataOutputStream outputStream = fs.create(path, (short) 2);
        
        outputStream.write("Hello replicated world!".getBytes());
        outputStream.flush();
        
        outputStream.close();
    }
    
    private static void testLargeFileCreation() throws IOException {
        URI uri = URI.create("hdfs://192.168.140.129/user/rtershak/large.01.data");
        Configuration conf = new Configuration();
        FileSystem fs = FileSystem.get(uri, conf);
        
        FSDataOutputStream dos = fs.create(new Path(uri), (short) 2);
        
        BufferedOutputStream bufferedOutputStream = new BufferedOutputStream(dos);
        
        NumberFormat formatter = NumberFormat.getIntegerInstance();
        long i = 0;
        long total = 1024 * 1024 * 1024;
        while (i < total) {
            int b = (int) ((Math.random() * 60) + 65);
            if (b > 90) {
                b = '\n';
            }
            bufferedOutputStream.write(b);
            
            i++;
            if ((i % 1000000) == 0) {
                System.out.println(new StringBuilder().append(formatter.format(i)).append(" bytes written"));
            }
        }
        
        System.out.println(new StringBuilder().append(formatter.format(i)).append(" bytes written total"));
        System.out.println("Closing file");
        
        bufferedOutputStream.close();
    }
    
    private static void testReadingLargeFile() throws Exception {
        URI uri = URI.create("hdfs://192.168.140.129/user/rtershak/large.01.data");
        Configuration conf = new Configuration();
        FileSystem fs = FileSystem.get(uri, conf);
        
        FSDataInputStream inputStream = fs.open(new Path(uri));
        
        NumberFormat formater = NumberFormat.getIntegerInstance();
        
        long start = System.currentTimeMillis();
        long cou = 0;
        while (inputStream.read() != -1) {
            cou++;
            
            if (cou % 1000000 == 0) {
                System.out.println(formater.format(cou) + " bytes read");
            }
        }
        System.out.println(formater.format(cou) + " bytes read total");
        inputStream.close();
        
        System.out.println((System.currentTimeMillis() - start) / 1000 + " sec - total time");
    }
}
