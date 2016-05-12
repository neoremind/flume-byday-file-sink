package com.baidu.unbiz.flume.sink;

import java.io.OutputStream;
import java.util.Date;

import org.apache.flume.serialization.EventSerializer;

/**
 * @author zhangxu
 */
public class OutputStreamWrapper {

    private OutputStream outputStream;

    private Date date;

    private EventSerializer serializer;

    public OutputStream getOutputStream() {
        return outputStream;
    }

    public void setOutputStream(OutputStream outputStream) {
        this.outputStream = outputStream;
    }

    public Date getDate() {
        return date;
    }

    public void setDate(Date date) {
        this.date = date;
    }

    public EventSerializer getSerializer() {
        return serializer;
    }

    public void setSerializer(EventSerializer serializer) {
        this.serializer = serializer;
    }
}
