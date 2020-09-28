package net.venusoft.duration;

import lombok.Data;
import org.apache.hadoop.io.Writable;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

/**
 * @author lk
 * @date 2020/9/28 12:04
 */
@Data
public class AccessResultBean implements Writable {
    private int count;
    private int sum;
    private double avg;

    public AccessResultBean(int count, int sum, double avg) {
        this.count = count;
        this.sum = sum;
        this.avg = avg;
    }

    @Override
    public String toString() {
        return count + "\t" + sum + "\t" + avg;
    }

    @Override
    public void write(DataOutput out) throws IOException {
        out.writeInt(count);
        out.writeInt(sum);
        out.writeDouble(avg);
    }

    @Override
    public void readFields(DataInput in) throws IOException {
        this.count = in.readInt();
        this.sum = in.readInt();
        this.avg = in.readInt();
    }
}
