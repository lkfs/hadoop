package net.venusoft.count;

import lombok.extern.slf4j.Slf4j;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

/**
 * @author lk
 * @date 2020/9/27 14:54
 */
@Slf4j
public class AccessCountMapper extends Mapper<LongWritable, Text, Text, IntWritable> {
    //private Pattern requesPattern = Pattern.compile("\\[(.*?)\\].*\\[Request\\]\\s*?(LOG\\d*)\\s*(.*?)\\([0-9\\.]*\\)");
    private Pattern requesPattern = Pattern.compile("(\\d{4}-\\d{2}-\\d{2} \\d{2}:\\d{2}:\\d{2}\\.\\d{3})\\s*\\[(\\d*)\\].*path:\\[(.+?)\\] took time");
    @Override
    protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
        String line = value.toString();
        Matcher matcherRequest = requesPattern.matcher(line);
        if(matcherRequest.find()){
            String time = matcherRequest.group(1);
            String logId = matcherRequest.group(2);
            String path = matcherRequest.group(3);

            log.info("find request time = {}, logId = {},  path = {}", time, logId, path);
            context.write(new Text(path), new IntWritable(1));
        }
    }
}
