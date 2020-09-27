package net.venusoft.duration;

import lombok.extern.slf4j.Slf4j;
import org.apache.commons.lang3.RandomUtils;
import org.apache.commons.lang3.StringUtils;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;
import java.time.Duration;
import java.time.LocalDateTime;
import java.time.format.DateTimeFormatter;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

/**
 * @author lk
 * @date 2020/9/21 15:52
 */
@Slf4j
public class AccessDurationMapper extends Mapper<LongWritable, Text, Text, IntWritable> {
    private Pattern requesPattern = Pattern.compile("\\[(.*?)\\].*\\[Request\\]\\s*?(LOG\\d*)\\s*(.*?)\\([0-9\\.]*\\)");
    private Pattern responsePattern = Pattern.compile("\\[(.*?)\\].*\\[RESPONSE\\]\\s*?(LOG\\d*)");
    private Map<String, List<String>> requestMap = new HashMap<>();
    @Override
    protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
        String line = value.toString();
        Matcher matcherRequest = requesPattern.matcher(line);
        if(matcherRequest.find()){
            String time = matcherRequest.group(1);
            String logId = matcherRequest.group(2);
            String path = matcherRequest.group(3);

            log.info("+++++ requestMap.put logId:{} path:{}", logId, path);
            //requestMap.put(logId, Arrays.asList(time, path));

            context.write(new Text(path), new IntWritable(RandomUtils.nextInt(1,7)));
        }
        else{
            context.write(new Text(""), new IntWritable(RandomUtils.nextInt(1,7)));

            /*Matcher matcherResponse = responsePattern.matcher(line);
            if(matcherResponse.find()){
                String endTimeStr = matcherResponse.group(1);
                String logId = matcherResponse.group(2);
                List<String> request = requestMap.get(logId);
                if(request!=null){
                    String time = request.get(0);
                    String path = request.get(1);

                    LocalDateTime startTime = LocalDateTime.parse(time, DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss"));
                    LocalDateTime endTime = LocalDateTime.parse(endTimeStr, DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss"));
                    Long seconds = Duration.between(startTime, endTime).getSeconds();

                    log.info("+++++ context.write path:{}", path);
                    //context.write(new Text(path), new IntWritable(seconds.intValue()));
                }
            }*/
        }


    }

    /*public static void main(String[] args) {
        //
        Pattern pattern = Pattern.compile("\\[(.*?)\\].*\\[RESPONSE\\]\\s*?(LOG\\d*)");
        String line = "[2020-09-19 12:29:25] production.INFO: [RESPONSE]  LOG202009191229257993505  App\\Components\\Common\\ApiResponse::makeResponse  {\"code\":901,\"result\":false,\"message\":\"\\u7f3a\\u5c11\\u53c2\\u6570\",\"ret\":{\"data\":\"UeQLQGzhvrQMJA3Fi0C8tLCza8ykoImqaa2OYjWQPR86QtYu8ySF0nbX92nx1wMH\"}}";
        Matcher matcher = pattern.matcher(line);
        if(matcher.find()){
            System.out.println("success");
            String time = matcher.group(1);
            String logId = matcher.group(2);
            log.info("time = {}, logId = {}", time, logId);
        }
        else{
            System.out.println("not founc");
        }

    }*/
}
