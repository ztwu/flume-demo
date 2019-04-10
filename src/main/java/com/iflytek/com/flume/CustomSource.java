package com.iflytek.com.flume;

import org.apache.flume.Context;
import org.apache.flume.EventDeliveryException;
import org.apache.flume.PollableSource;
import org.apache.flume.conf.Configurable;
import org.apache.flume.event.EventBuilder;
import org.apache.flume.source.AbstractSource;

import java.nio.charset.Charset;
import java.util.HashMap;
import java.util.Random;

/**
 * created with idea
 * user:ztwu
 * date:2019/4/10
 * description
 * PollableSource是通过线程不断去调用process方法，主动拉取消息
 */
public class CustomSource extends AbstractSource implements Configurable,PollableSource{

    public long getBackOffSleepIncrement() {
        // TODO Auto-generated method stub
        return 0;
    }

    public long getMaxBackOffSleepInterval() {
        // TODO Auto-generated method stub
        return 0;
    }

    // 不断循环调用
    public PollableSource.Status process() throws EventDeliveryException {
        Random random = new Random();
        int randomNum = random.nextInt(100);
        String text = "Hello world" + random.nextInt(100);
        HashMap<String, String> header = new HashMap<String,String>();
        header.put("id",Integer.toString(randomNum));
        this.getChannelProcessor().processEvent(EventBuilder.withBody(text, Charset.forName("UTF-8"),header));
        return Status.READY;
    }

    public void configure(Context arg0) {

    }
}