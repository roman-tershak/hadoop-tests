package hadoop.tests;

import java.io.IOException;
import java.util.StringTokenizer;
import java.util.logging.Logger;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.MapReduceBase;
import org.apache.hadoop.mapred.Mapper;
import org.apache.hadoop.mapred.OutputCollector;
import org.apache.hadoop.mapred.Reporter;

public class Mapper01 extends MapReduceBase implements Mapper<LongWritable, Text, Text, IntWritable> {
    
    private final static Logger logger = Logger.getLogger(Mapper01.class.getSimpleName());

    private final static IntWritable one = new IntWritable(1);
    
    public void map(LongWritable key, Text value, OutputCollector<Text, IntWritable> output, Reporter reporter)
            throws IOException {
        
        String line = value.toString();
        
        logger.info(line);
//        System.out.println(line);
//        System.err.println(line);
        
        StringTokenizer tokenizer = new StringTokenizer(line);
        
        while (tokenizer.hasMoreTokens()) {
            Text word = new Text(tokenizer.nextToken());
            
            output.collect(word, one);
            
            logger.info("\t" + word + ", " + one);
//            System.out.println("\t" + word + ", " + one);
//            System.err.println("\t" + word + ", " + one);
        }
        
    }
}
