package com.baidu.unbiz.flume.sink;

import java.io.BufferedOutputStream;
import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.OutputStream;
import java.io.UnsupportedEncodingException;
import java.util.Calendar;
import java.util.Collection;
import java.util.Date;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;

import org.apache.commons.lang.StringUtils;
import org.apache.commons.lang.time.DateUtils;
import org.apache.flume.Channel;
import org.apache.flume.Context;
import org.apache.flume.Event;
import org.apache.flume.EventDeliveryException;
import org.apache.flume.Transaction;
import org.apache.flume.conf.Configurable;
import org.apache.flume.formatter.output.PathManager;
import org.apache.flume.instrumentation.SinkCounter;
import org.apache.flume.serialization.EventSerializer;
import org.apache.flume.serialization.EventSerializerFactory;
import org.apache.flume.sink.AbstractSink;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.base.Preconditions;
import com.google.common.collect.Maps;
import com.google.common.util.concurrent.ThreadFactoryBuilder;

/**
 * 把<code>Event</code>写入文件的sink
 * <p/>
 * 这里有两个条件：<br/>
 * 1）Event header中必须有timestamp，否则会忽略事件，并且会抛出{@link InputNotSpecifiedException} <br/>
 * 2）Event body如果是按照<code>##$$##</code>分隔的，那么把分隔之前的字符串当做模块名称（module name）来处理；如果没有则默认为default文件名<br/>
 * <p/>
 * 输出到本地文件，首先要设置一个跟目录，通过<code>sink.directory</code>设置。
 * 其次根据条件#2中提取出来的module name作为文件名称前缀，timestamp日志作为文件名称后缀，例如文件名为portal.20150606或者default.20150703。
 * <p/>
 * NOTE:具有rolling by day的功能，文件会按照timestamp进行天级别粒度的存储。
 *
 * @author zhangxu
 */
public class RollingByTypeAndDayFileSink extends AbstractSink implements Configurable {

    private static final Logger logger = LoggerFactory.getLogger(RollingByTypeAndDayFileSink.class);

    private static final int defaultBatchSize = 100;

    private int batchSize = defaultBatchSize;

    private String directory;
    private ConcurrentMap<String, OutputStreamWrapper> fileName2OutputStream = Maps.newConcurrentMap();

    private String serializerType;
    private Context serializerContext;

    private SinkCounter sinkCounter;

    public RollingByTypeAndDayFileSink() {
    }

    @Override
    public void configure(Context context) {
        String directory = context.getString("sink.directory");

        serializerType = context.getString("sink.serializer", "TEXT");
        serializerContext =
                new Context(context.getSubProperties("sink." +
                        EventSerializer.CTX_PREFIX));

        Preconditions.checkArgument(directory != null, "Directory may not be null");
        Preconditions.checkNotNull(serializerType, "Serializer type is undefined");

        batchSize = context.getInteger("sink.batchSize", defaultBatchSize);

        this.directory = directory;

        if (sinkCounter == null) {
            sinkCounter = new SinkCounter(getName());
        }
    }

    @Override
    public void start() {
        logger.info("Starting {}...", this);
        sinkCounter.start();
        super.start();
        logger.info("RollingByTypeAndDaySink {} started.", getName());
    }

    @Override
    public Status process() throws EventDeliveryException {
        Channel channel = getChannel();
        Transaction transaction = channel.getTransaction();
        Event event = null;
        Status result = Status.READY;

        try {
            transaction.begin();
            int eventAttemptCounter = 0;
            OutputStreamWrapper outputStreamWrapper = null;
            for (int i = 0; i < batchSize; i++) {
                event = channel.take();
                if (event != null) {
                    sinkCounter.incrementEventDrainAttemptCount();
                    eventAttemptCounter++;

                    String moduleName = getModuleName(event);

                    Date date = getDate(event.getHeaders());
                    outputStreamWrapper = fileName2OutputStream.get(moduleName);
                    if (outputStreamWrapper == null) {
                        outputStreamWrapper = createOutputStreamWrapper(moduleName, date);
                        fileName2OutputStream.put(moduleName, outputStreamWrapper);
                    } else {
                        if (!DateUtil.isSameDay(outputStreamWrapper.getDate(), date)) {
                            logger.debug("Time to rotate {}", getFileByModuleName(moduleName, date));
                            destroyOutputStreamWrapper(outputStreamWrapper, moduleName, date);
                            outputStreamWrapper = createOutputStreamWrapper(moduleName, date);
                            fileName2OutputStream.put(moduleName, outputStreamWrapper);
                        }
                    }

                    outputStreamWrapper.getSerializer().write(event);
                } else {
                    // No events found, request back-off semantics from runner
                    result = Status.BACKOFF;
                    break;
                }
            }
            if (outputStreamWrapper != null) {
                outputStreamWrapper.getSerializer().flush();
                outputStreamWrapper.getOutputStream().flush();
            }
            transaction.commit();
            sinkCounter.addToEventDrainSuccessCount(eventAttemptCounter);
        } catch (InputNotSpecifiedException ex) {
            transaction.rollback();
            //logger.error(ex.getMessage());
        } catch (Exception ex) {
            transaction.rollback();
            throw new EventDeliveryException("Failed to process transaction", ex);
        } finally {
            transaction.close();
        }

        return result;
    }

    @Override
    public void stop() {
        logger.info("RollingByTypeAndDay sink {} stopping...", getName());
        sinkCounter.stop();
        super.stop();
        Collection<OutputStreamWrapper> outputStreamWrapperCollection = fileName2OutputStream.values();

        if (outputStreamWrapperCollection != null) {
            for (OutputStreamWrapper outputStreamWrapper : outputStreamWrapperCollection) {
                destroyOutputStreamWrapper(outputStreamWrapper);
            }
        }
        logger.info("RollingByTypeAndDay sink {} stopped. Event metrics: {}", getName(), sinkCounter);
    }

    private String getModuleName(Event event) {
        try {
            String line = new String(event.getBody(), "UTF-8");
            String[] seps = line.split("##\\$\\$##");
            if (seps != null && seps.length == 2) {
                if (StringUtils.isNotEmpty(seps[1])) {
                    event.setBody(seps[1].getBytes("UTF-8"));
                }
                return seps[0];
            }
        } catch (UnsupportedEncodingException e) {
            logger.error(e.getMessage());
        }
        return "default";
    }

    private Date getDate(Map<String, String> eventHeaders) {
        String timestamp = eventHeaders.get("timestamp");
        if (StringUtils.isEmpty(timestamp)) {
            throw new InputNotSpecifiedException("timestamp cannot be found in event header");
        }
        long millis = 0L;
        if (!StringUtils.isEmpty(timestamp)) {
            try {
                millis = Long.parseLong(timestamp);
            } catch (Exception e) {
                throw new InputNotSpecifiedException("timestamp cannot be parsed in event header");
            }
        }
        Calendar calendar = Calendar.getInstance();
        calendar.setTimeInMillis(millis);
        return calendar.getTime();
    }

    public OutputStreamWrapper createOutputStreamWrapper(String moduleName, Date date) throws EventDeliveryException {
        OutputStreamWrapper outputStreamWrapper = new OutputStreamWrapper();
        File currentFile = getFileByModuleName(moduleName, date);
        logger.debug("Opening output stream for file {}", currentFile);
        try {
            OutputStream outputStream = new BufferedOutputStream(
                    new FileOutputStream(currentFile));
            EventSerializer serializer = EventSerializerFactory.getInstance(
                    serializerType, serializerContext, outputStream);
            serializer.afterCreate();
            outputStreamWrapper.setOutputStream(outputStream);
            outputStreamWrapper.setSerializer(serializer);
            outputStreamWrapper.setDate(date);
            sinkCounter.incrementConnectionCreatedCount();
        } catch (IOException e) {
            sinkCounter.incrementConnectionFailedCount();
            throw new EventDeliveryException("Failed to open file "
                    + getFileByModuleName(moduleName, date) + " while delivering event", e);
        }
        return outputStreamWrapper;
    }

    public void destroyOutputStreamWrapper(OutputStreamWrapper outputStreamWrapper) {
        try {
            destroyOutputStreamWrapper(outputStreamWrapper, "", new Date());
        } catch (EventDeliveryException e) {
            // omit
        }
    }

    public void destroyOutputStreamWrapper(OutputStreamWrapper outputStreamWrapper, String moduleName, Date date)
            throws EventDeliveryException {
        if (outputStreamWrapper.getOutputStream() != null) {
            logger.debug("Closing file {}", getFileByModuleName(moduleName, date));

            try {
                outputStreamWrapper.getSerializer().flush();
                outputStreamWrapper.getSerializer().beforeClose();
                outputStreamWrapper.getOutputStream().close();
                sinkCounter.incrementConnectionClosedCount();
            } catch (IOException e) {
                sinkCounter.incrementConnectionFailedCount();
                throw new EventDeliveryException("Unable to rotate file "
                        + getFileByModuleName(moduleName, date) + " while delivering event", e);
            } finally {
                outputStreamWrapper.setOutputStream(null);
                outputStreamWrapper.setSerializer(null);
            }
        }
        outputStreamWrapper = null;
    }

    /**
     * 返回应该写入的文件句柄，为${module}.YYYYMMDD
     *
     * @param moduleName 模块名称
     * @param date       日期
     *
     * @return 文件
     */
    public File getFileByModuleName(String moduleName, Date date) {
        return new File(this.directory, moduleName + "." + DateUtil.formatDate(date));
    }

    public String getDirectory() {
        return directory;
    }

    public void setDirectory(String directory) {
        this.directory = directory;
    }

}
