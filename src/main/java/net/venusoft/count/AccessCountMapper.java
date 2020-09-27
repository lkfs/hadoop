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
    Pattern requesPattern = Pattern.compile("(\\d{4}-\\d{2}-\\d{2} \\d{2}:\\d{2}:\\d{2}\\.\\d{3})\\s*\\[(\\d*)\\].*path:\\[(.+?)\\] took time");
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

    public static void main(String[] args) {
        String s = "2020-09-12 09:06:41.684 [1599872801593] [http-nio-8089-exec-8] INFO  com.qrqy.ibd.config.AopConfig - class:[GetConfigController],method:[getConfig],path:[/getConfig] took time:[91ms],response ";
        Pattern requesPattern = Pattern.compile("(\\d{4}-\\d{2}-\\d{2} \\d{2}:\\d{2}:\\d{2}\\.\\d{3})\\s*\\[(\\d*)\\].*path:\\[(.*?)\\]");
        Matcher matcherRequest = requesPattern.matcher(s);
        if(matcherRequest.find()){

            String time = matcherRequest.group(1);
            String logId = matcherRequest.group(2);
            String path = matcherRequest.group(3);
            log.info("found");
        }
        else{

            log.error("not found");
        }

    }
}
