package net.venusoft;

import lombok.extern.slf4j.Slf4j;

import java.io.IOException;
import java.net.URISyntaxException;

/**
 * @author lk
 * @date 2020/9/16 13:56
 */
@Slf4j
public class HdfsDemo {
    public static void main(String[] args) throws URISyntaxException, IOException, InterruptedException {
        HdfsUploadHandler handler = new HdfsUploadHandler();
        handler.upload("D:\\logs");
        log.info("SUCCESS");
    }
}
