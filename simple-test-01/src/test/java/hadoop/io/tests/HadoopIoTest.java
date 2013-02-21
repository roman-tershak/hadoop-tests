package hadoop.io.tests;

import java.io.ByteArrayOutputStream;
import java.io.DataOutputStream;
import java.io.IOException;
import java.nio.charset.CharacterCodingException;

import org.apache.commons.codec.binary.Hex;
import org.apache.hadoop.io.FloatWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.Writable;
import org.junit.Test;

public class HadoopIoTest {

    @Test
    public void testPrimitiveWritables() throws IOException {
        print(new IntWritable(-1356));
        print(new FloatWritable((float) -3.1415));
    }
    
    @Test
    public void testText() throws Exception {
        print(new Text("Hello Hadoop IO"));
        print(new Text("\u0041\u00DF\u6771\uD801\uDC00"));
    }

    private static byte[] serialize(Writable writable) throws IOException {
        ByteArrayOutputStream bout = new ByteArrayOutputStream();
        DataOutputStream dos = new DataOutputStream(bout);
        writable.write(dos);
        dos.close();
        return bout.toByteArray();
    }
    
    private static void print(byte[] bytes) {
        System.out.println(Hex.encodeHex(bytes));
    }
    
    private static void print(Writable writable) throws IOException {
        System.out.println(writable + " - " + new String(Hex.encodeHex(serialize(writable))));
    }
    
    private static void print(Text text) throws CharacterCodingException {
        StringBuilder sb = new StringBuilder();
        String s = text.toString();
        sb.append(text).append(" - ").append(s.codePointCount(0, s.length())).append(", ");
        sb.append(new String(Hex.encodeHex(text.copyBytes()))).append(", [");
        for (int i = 0; i < s.length(); i++) {
            if (i > 0) {
                sb.append(",");
            }
            sb.append(s.charAt(i)).append("->").append((int) s.charAt(i));
        }
        sb.append("]");
        System.out.println(sb.toString());
        
    }
}
