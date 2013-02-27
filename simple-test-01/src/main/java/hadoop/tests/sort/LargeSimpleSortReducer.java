package hadoop.tests.sort;

import java.io.IOException;
import java.util.Iterator;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.MapReduceBase;
import org.apache.hadoop.mapred.OutputCollector;
import org.apache.hadoop.mapred.Reducer;
import org.apache.hadoop.mapred.Reporter;

public class LargeSimpleSortReducer extends MapReduceBase implements Reducer<Text, Text, Text, Text> {
    
    private static final Text EMPTY_TEXT = new Text();
    
    public void reduce(Text key, Iterator<Text> values, OutputCollector<Text, Text> output, Reporter reporter) throws IOException {
        
        while (values.hasNext()) {
            output.collect(values.next(), EMPTY_TEXT);
        }

    }

}
