package hadoop.tests;

import java.io.IOException;
import java.util.Iterator;
import java.util.logging.Logger;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.mapred.MapReduceBase;
import org.apache.hadoop.mapred.OutputCollector;
import org.apache.hadoop.mapred.Reducer;
import org.apache.hadoop.mapred.Reporter;

public class LargeReducer01 extends MapReduceBase implements Reducer<IntWritable, IntWritable, IntWritable, IntWritable> {
    
    private final static Logger logger = Logger.getLogger(LargeReducer01.class.getSimpleName());

    public void reduce(IntWritable keyLength, Iterator<IntWritable> values, OutputCollector<IntWritable, IntWritable> output, Reporter reporter)
            throws IOException {
        
        int sum = 0;
        while (values.hasNext()) {
            sum++;
            values.next();
        }

        logger.info(new StringBuilder().append(keyLength).append(" chars length - ").append(sum).toString());
        
        output.collect(keyLength, new IntWritable(sum));
    }

}
