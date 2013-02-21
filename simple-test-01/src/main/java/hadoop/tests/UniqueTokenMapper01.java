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

public class UniqueTokenMapper01 extends MapReduceBase implements Mapper<LongWritable, Text, Text, IntWritable> {
    
    private final static Logger logger = Logger.getLogger(UniqueTokenMapper01.class.getSimpleName());

    private final static IntWritable ONE = new IntWritable(1);

    private int instTokenCounter = 0;
    private static int mapperClassCounter = 0;
    
    private Text word = new Text();
    
    public void map(LongWritable key, Text value, OutputCollector<Text, IntWritable> output, Reporter reporter)
            throws IOException {
        
        StringTokenizer tokenizer = new StringTokenizer(value.toString());
        
        while (tokenizer.hasMoreTokens()) {
            word.set(tokenizer.nextToken());
            
            output.collect(word , ONE);
            
            instTokenCounter++;
            if (instTokenCounter % 10000 == 0) {
                logger.info(new StringBuilder().append("Instance counter - ").append(instTokenCounter).append(" tokens").toString());
            }
            mapperClassCounter++;
            if (mapperClassCounter % 10000 == 0) {
                logger.info(new StringBuilder().append("Class counter - ").append(mapperClassCounter).append(" tokens").toString());
            }
        }
        
    }
}
