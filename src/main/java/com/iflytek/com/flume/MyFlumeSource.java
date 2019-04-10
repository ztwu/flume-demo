package com.iflytek.com.flume;

import org.apache.commons.io.FileUtils;
import org.apache.flume.Context;
import org.apache.flume.EventDrivenSource;
import org.apache.flume.channel.ChannelProcessor;
import org.apache.flume.conf.Configurable;
import org.apache.flume.event.EventBuilder;
import org.apache.flume.source.AbstractSource;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.RandomAccessFile;
import java.nio.charset.Charset;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;

/**
 * created with idea
 * user:ztwu
 * date:2019/4/10
 * description
 * EventDrivenSource是需要触发一个调用机制，即被动等待
 */
public class MyFlumeSource extends AbstractSource implements Configurable, EventDrivenSource {

    private static final Logger logger = LoggerFactory.getLogger(MyFlumeSource.class);
    //数据源的文件
    private String filePath;
    //保存偏移量的文件
    private String posiFile;
    //等待时长
    private Long interval;
    //编码格式
    private String charset;
    private FileRunnable fileRunnable;
    private ExecutorService pool;

    /**
     * 初始化Flume配置信息
     * @param context
     */
    public void configure(Context context) {
        filePath = context.getString("filePath");
        posiFile = context.getString("posiFile");
        interval = context.getLong("interval",2000L);
        charset = context.getString("charset","UTF-8");
    }

    public synchronized void start() {
        pool = Executors.newSingleThreadExecutor();
        fileRunnable = new FileRunnable(filePath,posiFile,interval,charset,getChannelProcessor());
        pool.execute(fileRunnable);
        super.start();
    }

    public synchronized void stop() {
        fileRunnable.setFlag(false);
        pool.shutdown();
        while (!pool.isTerminated()) {
            logger.debug("Waiting for exec executor service to stop");
            try {
                pool.awaitTermination(500, TimeUnit.MILLISECONDS);
            } catch (InterruptedException e) {
                logger.debug("Interrupted while waiting for exec executor service "
                        + "to stop. Just exiting.");
                Thread.currentThread().interrupt();
            }
        }
        super.stop();
    }
    private static class FileRunnable implements Runnable{
        private boolean flag = true;
        //偏移量
        private Long offset =0L;
        private Long interval;
        private String charset;
        //可以直接从偏移量开始读取数据
        private RandomAccessFile randomAccessFile;
        //可以发送给channel的工具类
        private ChannelProcessor channelProcessor;
        private File file;

        public void setFlag(boolean flag) {
            this.flag = flag;
        }

        public FileRunnable(String filePath, String posiFile, Long interval, String charset, ChannelProcessor channelProcessor) {
            this.interval = interval;
            this.charset = charset;
            this.channelProcessor = channelProcessor;
            file = new File(posiFile);
            if (!file.exists()){
                try {
                    file.createNewFile();
                } catch (IOException e) {
                    logger.error("create posiFile file error",e);
                }
            }
            try {
                String offsetStr = FileUtils.readFileToString(file);
                if (offsetStr != null && !"".equals(offsetStr)){
                    offset = Long.parseLong(offsetStr);
                }
            } catch (IOException e) {
                logger.error("read posiFile file error",e);
            }
            try {
                randomAccessFile = new RandomAccessFile(filePath,"r");
                randomAccessFile.seek(offset);

            } catch (FileNotFoundException e) {
                logger.error("read filePath file error",e);
            } catch (IOException e) {
                logger.error("randomAccessFile seek error",e);
            }
        }

        public void run() {
            while (flag){
                try {
                    String line = randomAccessFile.readLine();
                    if (line != null){
                        //向channel发送数据
                        channelProcessor.processEvent(EventBuilder.withBody(line, Charset.forName(charset)));
                        offset = randomAccessFile.getFilePointer();
                        FileUtils.writeStringToFile(file,offset.toString());
                    }else {
                        Thread.sleep(interval);
                    }
                } catch (IOException e) {
                    logger.error("read randomAccessFile error",e);
                } catch (InterruptedException e) {
                    logger.error("sleep error",e);
                }
            }
        }
    }
}