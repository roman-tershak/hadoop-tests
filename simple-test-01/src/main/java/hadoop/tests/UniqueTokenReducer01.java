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

public class UniqueTokenReducer01 extends MapReduceBase implements Reducer<Text, IntWritable, Text, IntWritable> {
    
    private final static Logger logger = Logger.getLogger(UniqueTokenReducer01.class.getSimpleName());

    private int instCounter = 0;
    private static int reducerClassCounter = 0;
    
    public void reduce(Text key, Iterator<IntWritable> values, OutputCollector<Text, IntWritable> output, Reporter reporter)
            throws IOException {
        
        int sum = 0;
        while (values.hasNext()) {
            values.next();
            sum++;
        }
        
        if (sum != 1) {
            output.collect(key, new IntWritable(sum));
        }
        
        instCounter++;
        if (instCounter % 10000 == 0) {
            logger.info(new StringBuilder().append("Instance counter - ").append(instCounter).append(" unique tokens").toString());
        }
        reducerClassCounter++;
        if (reducerClassCounter % 10000 == 0) {
            logger.info(new StringBuilder().append("Class counter - ").append(reducerClassCounter).append(" unique tokens").toString());
        }
    }

}
