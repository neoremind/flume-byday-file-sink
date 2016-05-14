package com.baidu.unbiz.flume.sink;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.nio.charset.Charset;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.TimeUnit;

import org.apache.commons.lang.StringUtils;
import org.apache.flume.Context;
import org.apache.flume.Event;
import org.apache.flume.EventDrivenSource;
import org.apache.flume.SystemClock;
import org.apache.flume.channel.ChannelProcessor;
import org.apache.flume.conf.Configurable;
import org.apache.flume.event.EventBuilder;
import org.apache.flume.instrumentation.SourceCounter;
import org.apache.flume.source.AbstractSource;
import org.apache.flume.source.ExecSourceConfigurationConstants;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.base.Preconditions;
import com.google.common.util.concurrent.ThreadFactoryBuilder;

/**
 * 带有默认前缀字符串加入ExecSource输出结果的{@link EventDrivenSource}
 * <p/>
 * 配合tail -F 执行的结果加入固定的前缀，也就是放到event body中。前缀不通过static interceptor放入event header。
 * <p/>
 * 例如将输出加入<tt>portal##$$##10.18.24.33</tt>这个默认前缀的配置方法如下：
 * <pre>
 * a1.sources.r1.type = com.baidu.unbiz.flume.sink.PrefixExecSource
 * a1.sources.r1.command = tail -F /home/work/zhangxu/flume/logback.log
 * a1.sources.r1.channels = c1
 * a1.sources.r1.prefix=portal
 * a1.sources.r1.separator=##$$##
 * a1.sources.r1.suffix=10.18.24.33
 * </pre>
 * 原始文件某行为
 * <tt>abcdefg</tt>，那么日志的输出会变成
 * <tt>portal##$$##10.18.24.33 \t abcdefg</tt>
 * <p/>
 * 另外，有prefix，suffix才生效，否则suffix不会单独存在。
 * <p/>
 * 最高优先级的配置方式是通过JVM参数加入-Dflume_execsource_prefix、-Dflume_execsource_separator和-Dflume_execsource_suffix，
 * 其次是环境变量中的flume_execsource_prefix、flume_execsource_separator和flume_execsource_suffix
 * <p/>
 * Note：由于flume的{@link org.apache.flume.source.ExecSource}没有hook的好方式，只能copy <tt>1.0.6.</tt>的源代码。
 *
 * @author zhangxu
 */
public class StaticLinePrefixExecSource extends AbstractSource implements EventDrivenSource, Configurable {

    private static final Logger logger = LoggerFactory.getLogger(StaticLinePrefixExecSource.class);

    private String shell;
    private String command;
    private SourceCounter sourceCounter;
    private ExecutorService executor;
    private Future<?> runnerFuture;
    private long restartThrottle;
    private boolean restart;
    private boolean logStderr;
    private Integer bufferCount;
    private long batchTimeout;
    private ExecRunnable runner;
    private Charset charset;

    /**
     * 前缀
     */
    private String prefix;

    /**
     * 前缀和后缀之间的分隔符
     */
    private String separator;

    /**
     * 后缀
     */
    private String suffix;

    /**
     * {@link #prefix} + {@link #separator} + {@link #suffix}的结果
     */
    private String linePrefix;

    @Override
    public void start() {
        logger.info("Exec source starting with command:{}", command);

        executor = Executors.newSingleThreadExecutor();

        runner = new ExecRunnable(shell, command, getChannelProcessor(), sourceCounter,
                restart, restartThrottle, logStderr, bufferCount, batchTimeout, charset, linePrefix);

        // FIXME: Use a callback-like executor / future to signal us upon failure.
        runnerFuture = executor.submit(runner);

    /*
     * NB: This comes at the end rather than the beginning of the method because
     * it sets our state to running. We want to make sure the executor is alive
     * and well first.
     */
        sourceCounter.start();
        super.start();

        logger.debug("Exec source started");
    }

    @Override
    public void stop() {
        logger.info("Stopping exec source with command:{}", command);
        if (runner != null) {
            runner.setRestart(false);
            runner.kill();
        }

        if (runnerFuture != null) {
            logger.debug("Stopping exec runner");
            runnerFuture.cancel(true);
            logger.debug("Exec runner stopped");
        }
        executor.shutdown();

        while (!executor.isTerminated()) {
            logger.debug("Waiting for exec executor service to stop");
            try {
                executor.awaitTermination(500, TimeUnit.MILLISECONDS);
            } catch (InterruptedException e) {
                logger.debug("Interrupted while waiting for exec executor service "
                        + "to stop. Just exiting.");
                Thread.currentThread().interrupt();
            }
        }

        sourceCounter.stop();
        super.stop();

        logger.debug("Exec source with command:{} stopped. Metrics:{}", command,
                sourceCounter);
    }

    @Override
    public void configure(Context context) {
        command = context.getString("command");

        Preconditions.checkState(command != null,
                "The parameter command must be specified");

        restartThrottle = context.getLong(ExecSourceConfigurationConstants.CONFIG_RESTART_THROTTLE,
                ExecSourceConfigurationConstants.DEFAULT_RESTART_THROTTLE);

        restart = context.getBoolean(ExecSourceConfigurationConstants.CONFIG_RESTART,
                ExecSourceConfigurationConstants.DEFAULT_RESTART);

        logStderr = context.getBoolean(ExecSourceConfigurationConstants.CONFIG_LOG_STDERR,
                ExecSourceConfigurationConstants.DEFAULT_LOG_STDERR);

        bufferCount = context.getInteger(ExecSourceConfigurationConstants.CONFIG_BATCH_SIZE,
                ExecSourceConfigurationConstants.DEFAULT_BATCH_SIZE);

        batchTimeout = context.getLong(ExecSourceConfigurationConstants.CONFIG_BATCH_TIME_OUT,
                ExecSourceConfigurationConstants.DEFAULT_BATCH_TIME_OUT);

        charset = Charset.forName(context.getString(ExecSourceConfigurationConstants.CHARSET,
                ExecSourceConfigurationConstants.DEFAULT_CHARSET));

        shell = context.getString(ExecSourceConfigurationConstants.CONFIG_SHELL, null);
        prefix = context.getString("prefix", null);
        separator = context.getString("separator", null);
        suffix = context.getString("suffix", null);

        prefix = SystemPropertiesUtil.getSystemProperty("flume_execsource_prefix", "flume_execsource_prefix", prefix);
        separator =
                SystemPropertiesUtil.getSystemProperty("flume_execsource_separator", "flume_execsource_separator",
                        separator);
        suffix = SystemPropertiesUtil.getSystemProperty("flume_execsource_suffix", "flume_execsource_suffix", suffix);

        // prefix和separator必须有，然后如果有suffix则加上
        // 如果有suffix，默认后面加入\t分隔
        if (StringUtils.isNotEmpty(prefix) && StringUtils.isNotEmpty(separator)) {
            linePrefix = prefix + separator;
            if (StringUtils.isNotEmpty(suffix)) {
                linePrefix = linePrefix + suffix + "\t";
            }
        }

        if (sourceCounter == null) {
            sourceCounter = new SourceCounter(getName());
        }
    }

    private static class ExecRunnable implements Runnable {

        public ExecRunnable(String shell, String command, ChannelProcessor channelProcessor,
                            SourceCounter sourceCounter, boolean restart, long restartThrottle,
                            boolean logStderr, int bufferCount, long batchTimeout, Charset charset,
                            String linePrefix) {
            this.command = command;
            this.channelProcessor = channelProcessor;
            this.sourceCounter = sourceCounter;
            this.restartThrottle = restartThrottle;
            this.bufferCount = bufferCount;
            this.batchTimeout = batchTimeout;
            this.restart = restart;
            this.logStderr = logStderr;
            this.charset = charset;
            this.shell = shell;
            this.linePrefix = linePrefix;
        }

        private final String shell;
        private final String command;
        private final ChannelProcessor channelProcessor;
        private final SourceCounter sourceCounter;
        private volatile boolean restart;
        private final long restartThrottle;
        private final int bufferCount;
        private long batchTimeout;
        private final boolean logStderr;
        private final Charset charset;
        private Process process = null;
        private SystemClock systemClock = new SystemClock();
        private Long lastPushToChannel = systemClock.currentTimeMillis();
        ScheduledExecutorService timedFlushService;
        ScheduledFuture<?> future;

        private String linePrefix;

        @Override
        public void run() {
            do {
                String exitCode = "unknown";
                BufferedReader reader = null;
                String line = null;
                final List<Event> eventList = new ArrayList<Event>();

                timedFlushService = Executors.newSingleThreadScheduledExecutor(
                        new ThreadFactoryBuilder().setNameFormat(
                                "timedFlushExecService" +
                                        Thread.currentThread().getId() + "-%d").build());
                try {
                    if (shell != null) {
                        String[] commandArgs = formulateShellCommand(shell, command);
                        process = Runtime.getRuntime().exec(commandArgs);
                    } else {
                        String[] commandArgs = command.split("\\s+");
                        process = new ProcessBuilder(commandArgs).start();
                    }
                    reader = new BufferedReader(
                            new InputStreamReader(process.getInputStream(), charset));

                    // StderrLogger dies as soon as the input stream is invalid
                    StderrReader stderrReader = new StderrReader(new BufferedReader(
                            new InputStreamReader(process.getErrorStream(), charset)), logStderr);
                    stderrReader.setName("StderrReader-[" + command + "]");
                    stderrReader.setDaemon(true);
                    stderrReader.start();

                    future = timedFlushService.scheduleWithFixedDelay(
                            new Runnable() {
                                @Override
                                public void run() {
                                    try {
                                        synchronized(eventList) {
                                            if (!eventList.isEmpty()
                                                    && timeout()) {
                                                flushEventBatch(eventList);
                                            }
                                        }
                                    } catch (Exception e) {
                                        logger.error(
                                                "Exception occured when "
                                                        + "processing event"
                                                        + " batch",
                                                e);
                                        if (e instanceof
                                                InterruptedException) {
                                            Thread.currentThread()
                                                    .interrupt();
                                        }
                                    }
                                }
                            },
                            batchTimeout, batchTimeout, TimeUnit.MILLISECONDS);

                    while ((line = reader.readLine()) != null) {
                        synchronized(eventList) {
                            sourceCounter.incrementEventReceivedCount();

                            // 如果linePrefix不为空，则加入到某行的前面
                            if (linePrefix != null) {
                                eventList.add(EventBuilder.withBody((linePrefix + line).getBytes(charset)));
                            } else {
                                eventList.add(EventBuilder.withBody(line.getBytes(charset)));
                            }
                            if (eventList.size() >= bufferCount || timeout()) {
                                flushEventBatch(eventList);
                            }
                        }
                    }

                    synchronized(eventList) {
                        if (!eventList.isEmpty()) {
                            flushEventBatch(eventList);
                        }
                    }
                } catch (Exception e) {
                    logger.error("Failed while running command: " + command, e);
                    if (e instanceof InterruptedException) {
                        Thread.currentThread().interrupt();
                    }
                } finally {
                    if (reader != null) {
                        try {
                            reader.close();
                        } catch (IOException ex) {
                            logger.error("Failed to close reader for exec source", ex);
                        }
                    }
                    exitCode = String.valueOf(kill());
                }
                if (restart) {
                    logger.info("Restarting in {}ms, exit code {}", restartThrottle,
                            exitCode);
                    try {
                        Thread.sleep(restartThrottle);
                    } catch (InterruptedException e) {
                        Thread.currentThread().interrupt();
                    }
                } else {
                    logger.info("Command [" + command + "] exited with " + exitCode);
                }
            } while (restart);
        }

        private void flushEventBatch(List<Event> eventList) {
            channelProcessor.processEventBatch(eventList);
            sourceCounter.addToEventAcceptedCount(eventList.size());
            eventList.clear();
            lastPushToChannel = systemClock.currentTimeMillis();
        }

        private boolean timeout() {
            return (systemClock.currentTimeMillis() - lastPushToChannel) >= batchTimeout;
        }

        private static String[] formulateShellCommand(String shell, String command) {
            String[] shellArgs = shell.split("\\s+");
            String[] result = new String[shellArgs.length + 1];
            System.arraycopy(shellArgs, 0, result, 0, shellArgs.length);
            result[shellArgs.length] = command;
            return result;
        }

        public int kill() {
            if (process != null) {
                synchronized(process) {
                    process.destroy();

                    try {
                        int exitValue = process.waitFor();

                        // Stop the Thread that flushes periodically
                        if (future != null) {
                            future.cancel(true);
                        }

                        if (timedFlushService != null) {
                            timedFlushService.shutdown();
                            while (!timedFlushService.isTerminated()) {
                                try {
                                    timedFlushService.awaitTermination(500, TimeUnit.MILLISECONDS);
                                } catch (InterruptedException e) {
                                    logger.debug("Interrupted while waiting for exec executor service "
                                            + "to stop. Just exiting.");
                                    Thread.currentThread().interrupt();
                                }
                            }
                        }
                        return exitValue;
                    } catch (InterruptedException ex) {
                        Thread.currentThread().interrupt();
                    }
                }
                return Integer.MIN_VALUE;
            }
            return Integer.MIN_VALUE / 2;
        }

        public void setRestart(boolean restart) {
            this.restart = restart;
        }
    }

    private static class StderrReader extends Thread {
        private BufferedReader input;
        private boolean logStderr;

        protected StderrReader(BufferedReader input, boolean logStderr) {
            this.input = input;
            this.logStderr = logStderr;
        }

        @Override
        public void run() {
            try {
                int i = 0;
                String line = null;
                while ((line = input.readLine()) != null) {
                    if (logStderr) {
                        // There is no need to read 'line' with a charset
                        // as we do not to propagate it.
                        // It is in UTF-16 and would be printed in UTF-8 format.
                        logger.info("StderrLogger[{}] = '{}'", ++i, line);
                    }
                }
            } catch (IOException e) {
                logger.info("StderrLogger exiting", e);
            } finally {
                try {
                    if (input != null) {
                        input.close();
                    }
                } catch (IOException ex) {
                    logger.error("Failed to close stderr reader for exec source", ex);
                }
            }
        }
    }
}