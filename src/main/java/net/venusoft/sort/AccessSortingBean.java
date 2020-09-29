package net.venusoft.sort;

import lombok.Data;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.io.WritableComparable;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

/**
 * @author lk
 * @date 2020/9/28 12:04
 */
@Data
public class AccessSortingBean implements Writable /*WritableComparable<AccessSortingBean>*/ {
    private String path = "";
    private int count;
    private int sum;
    private double avg;

    public AccessSortingBean(String path, int count, int sum, double avg) {
        this.path = path;
        this.count = count;
        this.sum = sum;
        this.avg = avg;
    }

    @Override
    public String toString() {
        return path + "\t" + count + "\t" + sum + "\t" + avg;
    }

    /*@Override
    public int compareTo(AccessSortingBean o) {
        return o.avg > this.avg?1:(o.avg == this.avg?0:-1);
    }*/

    @Override
    public void write(DataOutput out) throws IOException {
        out.writeInt(count);
        out.writeInt(sum);
        out.writeDouble(avg);
//        out.writeUTF(path);
    }

    @Override
    public void readFields(DataInput in) throws IOException {
        this.count = in.readInt();
        this.sum = in.readInt();
        this.avg = in.readDouble();
//        this.path = in.readUTF();
    }

}
