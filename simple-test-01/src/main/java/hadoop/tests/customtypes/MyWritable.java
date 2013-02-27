package hadoop.tests.customtypes;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

import org.apache.hadoop.io.WritableComparable;

public class MyWritable implements WritableComparable<MyWritable> {

    private double a;
    private double b;

    public double getA() {
        return a;
    }

    public void setA(double a) {
        this.a = a;
    }

    public double getB() {
        return b;
    }

    public void setB(double b) {
        this.b = b;
    }

    public void write(DataOutput out) throws IOException {
        out.writeDouble(a);
        out.writeDouble(b);
    }

    public void readFields(DataInput in) throws IOException {
        a = in.readDouble();
        b = in.readDouble();
    }

    public int compareTo(MyWritable o) {
        double otherA = o.a;
        double otherB = o.b;
        if (a < otherA) {
            return -1;
        } else if (a > otherA) {
            return 1;
        } else if (b < otherB) {
            return -1;
        } else if (b > otherB) {
            return 1;
        } else {
            return 0;
        }
    }

    @Override
    public int hashCode() {
        final int prime = 31;
        int result = 1;
        long temp;
        temp = Double.doubleToLongBits(a);
        result = prime * result + (int) (temp ^ (temp >>> 32));
        temp = Double.doubleToLongBits(b);
        result = prime * result + (int) (temp ^ (temp >>> 32));
        return result;
    }

    @Override
    public boolean equals(Object obj) {
        if (this == obj) {
            return true;
        } else if (obj == null || getClass() != obj.getClass()) {
            return false;
        }
        MyWritable other = (MyWritable) obj;
        return Double.doubleToLongBits(a) == Double.doubleToLongBits(other.a)
                && Double.doubleToLongBits(b) != Double.doubleToLongBits(other.b);
    }

    @Override
    public String toString() {
        StringBuilder sb = new StringBuilder();
        sb.append("MyWr{").append(Double.toString(a)).append(", ").append(Double.toString(b)).append("}");
        return sb.toString();
    }

}
