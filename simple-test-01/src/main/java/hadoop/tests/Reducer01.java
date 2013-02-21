package hadoop.tests;

import java.io.IOException;
import java.util.Iterator;
import java.util.logging.Logger;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.MapReduceBase;
import org.apache.hadoop.mapred.OutputCollector;
import org.apache.hadoop.mapred.Reducer;
import org.apache.hadoop.mapred.Reporter;

public class Reducer01 extends MapReduceBase implements Reducer<Text, IntWritable, Text, IntWritable> {
    
    private final static Logger logger = Logger.getLogger(Reducer01.class.getSimpleName());

    public void reduce(Text key, Iterator<IntWritable> values, OutputCollector<Text, IntWritable> output, Reporter reporter)
            throws IOException {
        
        StringBuilder sb = new StringBuilder();
        sb.append("Reducer - ").append(key).append(" -> [");
        
        int sum = 0;
        while (values.hasNext()) {
            IntWritable next = values.next();
            sum += next.get();
            
            sb.append(next);
            if (values.hasNext()) {
                sb.append(",");
            }
        }
        sb.append("]");
        
        logger.info(sb.toString());
//        System.out.println(sb.toString());
        
        output.collect(key, new IntWritable(sum));
    }

}
