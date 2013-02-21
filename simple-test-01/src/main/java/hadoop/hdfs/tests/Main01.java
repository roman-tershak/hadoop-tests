package hadoop.hdfs.tests;

import java.io.BufferedInputStream;
import java.io.BufferedOutputStream;
import java.io.IOException;
import java.net.URI;
import java.text.NumberFormat;

import org.apache.commons.lang.builder.ToStringBuilder;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.FsStatus;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.util.Progressable;

public class Main01 {

    public static void main(String[] args) throws Exception {
//        testCreation();
//        testReading();
//        testWriting();
//        testLargeFile();
        testReadingLargeFile();
    }

    private static void testReading() throws IOException {
        Configuration conf = new Configuration();
//        conf.set("fs.default.name", "http://192.168.140.128");
        conf.set("dfs.http.address", "http://192.168.140.128");
        conf.set("dfs.datanode.http.address", "http://192.168.140.128");
//        URI uri = URI.create("hdfs://cloudera:cloudera@192.168.140.128/user/tests/f01");
        URI uri = URI.create("hdfs://192.168.140.128/user/rtershak/02.data");
        FileSystem fs = FileSystem.get(uri, conf);
        
//        fs.
        
        Path path = new Path(uri);
//        System.out.println("Exists - " + fs.exists(path));
//        
//        FsStatus fsStatus = fs.getStatus(path);
//        System.out.println("FsStatus - " + new ToStringBuilder(fsStatus)
//                .append("capacity", fsStatus.getCapacity())
//                .append("remaining", fsStatus.getRemaining())
//                .append("used", fsStatus.getUsed()).toString());
//        
//        FileStatus fileStatus = fs.getFileStatus(path);
//        ToStringBuilder tsb = new ToStringBuilder(fileStatus);
//        tsb.append("accessTime", fileStatus.getAccessTime());
//        tsb.append("blockSize", fileStatus.getBlockSize());
//        tsb.append("len", fileStatus.getLen());
//        tsb.append("modificationTime", fileStatus.getModificationTime());
//        tsb.append("replication", fileStatus.getReplication());
//        tsb.append("group", fileStatus.getGroup());
//        tsb.append("owner", fileStatus.getOwner());
//        tsb.append("path", fileStatus.getPath());
//        tsb.append("permission", fileStatus.getPermission());
//        System.out.println("FileStatus - " + tsb.toString());
        
        FSDataInputStream inputStream = fs.open(path, 0);
//        tsb = new ToStringBuilder(inputStream);
//        tsb.append("available", inputStream.available());
//        tsb.append("pos", inputStream.getPos());
//        tsb.append("markSupported", inputStream.markSupported());
//        tsb.append("fileDescriptor", inputStream.getFileDescriptor());
//        System.out.println("InputStream - " + tsb.toString());
        
        int counter = 0;
        int b;
        while ((b = inputStream.read()) != -1) {
            System.out.println("byte - " + b);
            counter++;
            if ((counter % 1000) == 0) {
                System.out.println(counter + " bytes are read");
            }
        }
        System.out.println("The end of the input stream reached. " + counter + " bytes are read in total");
        
        inputStream.close();
    }

    private static void testCreation() throws IOException {
        Configuration conf = new Configuration();
//        conf.set("dfs.http.address", "http://192.168.140.128");
//        conf.set("dfs.datanode.http.address", "http://192.168.140.128");
        URI uri = URI.create("hdfs://192.168.140.128/user/rtershak/02.data");
        FileSystem fs = FileSystem.get(uri, conf);
      
        Path path = new Path(uri);
        FSDataOutputStream outputStream = fs.create(path, (short) 1);
        
        outputStream.write("Hello world!".getBytes());
        outputStream.flush();
        
        outputStream.close();
    }
    
    private static void testWriting() throws IOException {
        Configuration conf = new Configuration();
//        conf.set("dfs.http.address", "http://192.168.140.128");
//        conf.set("dfs.datanode.http.address", "http://192.168.140.128");
//        conf.set("dfs.client.block.write.replace-datanode-on-failure.policy", "NEVER");
//        conf.set("dfs.client.block.write.replace-datanode-on-failure.enable", "false");
        URI uri = URI.create("hdfs://192.168.140.128/user/rtershak/02.data");
        FileSystem fs = FileSystem.get(uri, conf);
        
        Path path = new Path(uri);
        FSDataOutputStream outputStream = fs.append(path);
        
        outputStream.write("\nJust another 'Hello world!'\n".getBytes());
        outputStream.flush();
        
        outputStream.close();
    }
    
    private static void testLargeFile() throws IOException {
        URI uri = URI.create("hdfs://192.168.140.128/user/rtershak/large.data");
        Configuration conf = new Configuration();
        FileSystem fs = FileSystem.get(uri, conf);
        
        FSDataOutputStream dos = fs.create(new Path(uri), (short) 1);
        
        BufferedOutputStream bufferedOutputStream = new BufferedOutputStream(dos);
        
        NumberFormat formatter = NumberFormat.getIntegerInstance();
        long i = 0;
        while (i < 400000000L) {
            int b = (int) ((Math.random() * 60) + 32);
            if (b > 90) {
                b = '\n';
            }
            bufferedOutputStream.write(b);
            
            i++;
            if ((i % 1000000) == 0) {
                System.out.println(new StringBuilder().append(formatter.format(i)).append(" bytes written"));
            }
        }
        
        System.out.println(i + " bytes written");
        System.out.println("Closing file");
        
        bufferedOutputStream.close();
    }
    
    private static void testReadingLargeFile() throws Exception {
        URI uri = URI.create("hdfs://192.168.140.128/user/rtershak/large.data");
        Configuration conf = new Configuration();
        FileSystem fs = FileSystem.get(uri, conf);
        
        FSDataInputStream inputStream = fs.open(new Path(uri));
        
        NumberFormat formater = NumberFormat.getIntegerInstance();
        
        long start = System.currentTimeMillis();
        int cou = 0;
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
