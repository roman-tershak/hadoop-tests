package hadoop.tests.customtypes;

import java.io.IOException;
import java.util.Iterator;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.mapred.MapReduceBase;
import org.apache.hadoop.mapred.OutputCollector;
import org.apache.hadoop.mapred.Reducer;
import org.apache.hadoop.mapred.Reporter;

public class CustomTypeReducer extends MapReduceBase implements Reducer<MyWritable, IntWritable, MyWritable, IntWritable> {

    private IntWritable intWritable = new IntWritable();

    public void reduce(MyWritable key, Iterator<IntWritable> values, OutputCollector<MyWritable, IntWritable> output, 
            Reporter reporter) throws IOException {

        int sum = 0;
        while (values.hasNext()) {
            sum++;
            values.next();
        }
        
        intWritable.set(sum);
        output.collect(key, intWritable);
    }

}
