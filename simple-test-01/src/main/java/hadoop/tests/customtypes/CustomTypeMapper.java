package hadoop.tests.customtypes;

import java.io.IOException;
import java.util.Arrays;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.MapReduceBase;
import org.apache.hadoop.mapred.Mapper;
import org.apache.hadoop.mapred.OutputCollector;
import org.apache.hadoop.mapred.Reporter;

public class CustomTypeMapper extends MapReduceBase implements Mapper<LongWritable, Text, MyWritable, IntWritable> {

    private static final IntWritable ONE = new IntWritable(1);
    private MyWritable myWritable = new MyWritable();
    private byte[] secondPart = new byte[8];
    
    public void map(LongWritable key, Text value, OutputCollector<MyWritable, IntWritable> output, Reporter reporter)
            throws IOException {

        byte[] bytes = value.getBytes();
        myWritable.setA(bitsToDouble(bytes));
        
        int bytesLen = bytes.length;
        if (bytesLen > 8) {
            Arrays.fill(secondPart, (byte) 0);
            System.arraycopy(bytes, 8, secondPart, 0, bytesLen > 16 ? 8 : bytesLen - 8);
            
            myWritable.setB(bitsToDouble(secondPart));
        } else {
            myWritable.setB(0);
        }
        
        output.collect(myWritable, ONE);
    }

    private static double bitsToDouble(byte[] bytes) {
        return Double.longBitsToDouble(bitsToLong(bytes));
    }

    private static long bitsToLong(byte[] bytes) {
        int len = bytes.length > 8 ? 8 : bytes.length;
        
        long bits = 0;
        switch (len) {
        case 8:
            bits |= ((long) bytes[7] << 56);
        case 7:
            bits |= ((long) bytes[6] << 48 & 0x00FF000000000000L);
        case 6:
            bits |= ((long) bytes[5] << 40 & 0x0000FF0000000000L);
        case 5:
            bits |= ((long) bytes[4] << 32 & 0x000000FF00000000L);
        case 4:
            bits |= ((long) bytes[3] << 24 & 0x00000000FF000000L);
        case 3:
            bits |= ((long) bytes[2] << 16 & 0x0000000000FF0000L);
        case 2:
            bits |= ((long) bytes[1] << 8 & 0x0000000000000FF00L);
        case 1:
            bits |= ((long) bytes[0] & 0x000000000000000FFL);
        }
        return bits;
    }

    public static void main(String[] args) {
        System.out.println(bitsToLong(new byte[] { (byte) 0xd2, (byte) 0xad, (byte) 0xe3, (byte) 0xa4, 0x7e, 0x1d,
                (byte) 0xf2, (byte) 0xe2 }));
        System.out.println(bitsToDouble(new byte[] { (byte) 0xd2, (byte) 0xad, (byte) 0xe3, (byte) 0xa4, 0x7e, 0x1d,
                (byte) 0xf2, (byte) 0xe2 }));
    }
}
