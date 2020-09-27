package net.venusoft.count;

import lombok.extern.slf4j.Slf4j;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;
import java.time.Duration;
import java.time.LocalDateTime;
import java.time.format.DateTimeFormatter;
import java.util.HashMap;
import java.util.Map;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

/**
 * @author lk
 * @date 2020/9/27 14:54
 */
@Slf4j
public class AccessCountMapper extends Mapper<LongWritable, Text, Text, IntWritable> {
    private Map<String, String> map = new HashMap<>();
    private Pattern requestPattern = Pattern.compile("(\\d{4}-\\d{2}-\\d{2} \\d{2}:\\d{2}:\\d{2}\\.\\d{3})\\s*\\[(\\d*)\\].*path:\\[(.+?)\\],request");
    private Pattern responsePattern = Pattern.compile("(\\d{4}-\\d{2}-\\d{2} \\d{2}:\\d{2}:\\d{2}\\.\\d{3})\\s*\\[(\\d*)\\].*path:\\[(.+?)\\] took time:\\[\\d*ms\\],response");
    @Override
    protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
        String line = value.toString();
        Matcher matcherRequest = requestPattern.matcher(line);
        if(matcherRequest.find()){
            String time = matcherRequest.group(1);
            String logId = matcherRequest.group(2);
            String path = matcherRequest.group(3);
            log.info("find request time = {}, logId = {},  path = {}", time, logId, path);
            map.put(logId, time);
        }
        else{
            Matcher matcherResponse = responsePattern.matcher(line);
            if(matcherResponse.find()){
                String time = matcherResponse.group(1);
                String logId = matcherResponse.group(2);
                String path = matcherResponse.group(3);

                log.info("find response time = {}, logId = {},  path = {}", time, logId, path);

                String start = map.get(logId);
                if(start!=null){

                    LocalDateTime startTime = LocalDateTime.parse(start, DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss.SSS"));
                    LocalDateTime endTime = LocalDateTime.parse(time, DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss.SSS"));
                    Long milliSeconds = Duration.between(startTime, endTime).toMillis();

                    context.write(new Text(path), new IntWritable(new Long(milliSeconds).intValue()));
                }
            }
        }
    }

}
