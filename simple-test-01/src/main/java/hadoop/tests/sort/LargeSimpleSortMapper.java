package hadoop.tests.sort;

import java.io.IOException;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.MapReduceBase;
import org.apache.hadoop.mapred.Mapper;
import org.apache.hadoop.mapred.OutputCollector;
import org.apache.hadoop.mapred.Reporter;

public class LargeSimpleSortMapper extends MapReduceBase implements Mapper<LongWritable, Text, Text, Text> {
    
    private Text text = new Text();
    
    public void map(LongWritable key, Text value, OutputCollector<Text, Text> output, Reporter reporter) throws IOException {

        text.set(value);
        output.collect(text, text);
    }
}
