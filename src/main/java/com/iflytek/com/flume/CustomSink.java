package com.iflytek.com.flume;

import org.apache.flume.*;
import org.apache.flume.conf.Configurable;
import org.apache.flume.sink.AbstractSink;

/**
 * created with idea
 * user:ztwu
 * date:2019/4/10
 * description
 */
public class CustomSink extends AbstractSink implements Configurable {

    // 不断循环调用
    public Status process() throws EventDeliveryException {

        Status status = Status.READY;
        Transaction trans = null;
        try {
            Channel channel = getChannel();
            trans = channel.getTransaction();
            trans.begin();
            for(int i= 0;i < 100 ;i++) {
                Event event = channel.take();
                if(event == null) {
                    status = status.BACKOFF;
                    break;
                }else {
                    String body = new String(event.getBody());
                    System.out.println(body);
                }
            }
            trans.commit();
        }catch (Exception e) {
            if(trans != null) {
                trans.commit();
            }
            e.printStackTrace();
        }finally {
            if(trans != null) {
                trans.close();
            }
        }
        return status;
    }

    public void configure(Context arg0) {

    }
}