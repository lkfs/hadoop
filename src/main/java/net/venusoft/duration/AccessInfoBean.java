package net.venusoft.duration;

import lombok.Data;
import org.apache.hadoop.io.Writable;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.time.LocalDateTime;
import java.time.ZoneOffset;
import java.time.format.DateTimeFormatter;

/**
 * @author lk
 * @date 2020/9/28 12:04
 */
@Data
public class AccessInfoBean implements Writable {
    private static final String pattern = "yyyy-MM-dd HH:mm:ss.SSS";
    private String path;
    private Long startTime;
    private Long endTime;
    private int duration;

    public AccessInfoBean() {
    }

    public AccessInfoBean(String path, String startTime) {
        this.path = path;
        LocalDateTime ldt = LocalDateTime.parse(startTime, DateTimeFormatter.ofPattern(pattern));
        this.startTime = ldt.toInstant(ZoneOffset.of("+8")).toEpochMilli();
    }

    public void setEndTime(String endTime) {
        LocalDateTime ldt = LocalDateTime.parse(endTime, DateTimeFormatter.ofPattern(pattern));
        this.endTime = ldt.toInstant(ZoneOffset.of("+8")).toEpochMilli();
    }

    @Override
    public void write(DataOutput out) throws IOException {
        out.writeLong(startTime);
        out.writeLong(endTime);
        out.writeInt(duration);
        out.writeUTF(path);
    }

    @Override
    public void readFields(DataInput in) throws IOException {
        this.startTime = in.readLong();
        this.endTime = in.readLong();
        this.duration = in.readInt();
        this.path = in.readUTF();
    }
}
