package hadoop.tests;

import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.FileInputFormat;
import org.apache.hadoop.mapred.FileOutputFormat;
import org.apache.hadoop.mapred.JobClient;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.TextInputFormat;
import org.apache.hadoop.mapred.TextOutputFormat;

public class Main {

    public static void main(String[] args) throws Exception {
        JobConf jobConf = new JobConf();
        jobConf.setJobName("word_count");
        
        jobConf.setMapperClass(Mapper01.class);
        jobConf.setReducerClass(Reducer01.class);
        
        jobConf.setJar("/var/tmp/simple-test-01.jar");
        
        jobConf.setInputFormat(TextInputFormat.class);
        jobConf.setOutputFormat(TextOutputFormat.class);
        
        jobConf.setOutputKeyClass(Text.class);
        jobConf.setOutputValueClass(IntWritable.class);
        
        FileInputFormat.setInputPaths(jobConf, "/user/tests/f01");
        FileOutputFormat.setOutputPath(jobConf, new Path("/user/tests/f01.out"));
        
        JobClient.runJob(jobConf);
    }

}
