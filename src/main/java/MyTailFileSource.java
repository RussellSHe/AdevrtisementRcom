import org.apache.commons.io.FileUtils;
import org.apache.commons.lang.StringUtils;
import org.apache.flume.Context;
import org.apache.flume.Event;
import org.apache.flume.EventDrivenSource;
import org.apache.flume.channel.ChannelProcessor;
import org.apache.flume.conf.Configurable;
import org.apache.flume.event.EventBuilder;
import org.apache.flume.source.AbstractSource;
import org.apache.flume.source.ExecSource;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.*;
import java.nio.charset.Charset;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;

/**
 * User: Russell
 * Date: 2018-06-07
 * Time: 15:23
 */
public class MyTailFileSource extends AbstractSource implements Configurable, EventDrivenSource {
    private static final Logger logger = LoggerFactory.getLogger(MyTailFileSource.class);
    private  String filePath;

    private  String offsetFile;

    private  Long interval;
    private  String charset;

    private ChannelProcessor channelProcessor;
    private  myTask filetask;
    private  ExecutorService executor;

    /**
     * 在构造方法后执行一次configure方法
     *
     * @param context
     */
    public void configure(Context context) {
        filePath = context.getString("filePath");
        offsetFile = context.getString("offsetFile");
        interval = context.getLong("interval", 1000L);
        charset = context.getString("charset", "UTF-8");


    }

    /**
     * configure方法执行后在执行start方法
     */
    @Override
    public synchronized void start() {
        //创建一个单线程线程池
         executor = Executors.newSingleThreadExecutor();
        //创建一个实现了Runable接口的实现类
         filetask = new myTask(offsetFile,filePath,interval,charset,channelProcessor);
        //将实现类放入到线程池中
        executor.execute(filetask);

        super.start();
    }

    /**
     * 停止flume时执行一次
     */
    @Override
    public synchronized void stop() {
        //关掉线程
     filetask.setflag(false);
        //关掉线程池
        executor.shutdown();
        while (!executor.isTerminated()){
            logger.debug("Waiting for exec executor service to stop");
            try {
                executor.awaitTermination(500, TimeUnit.MILLISECONDS);
            } catch (InterruptedException e) {
                logger.debug("Interrupted while waiting for exec executor service "
                        + "to stop. Just exiting.");
                Thread.currentThread().interrupt();
            }

        }


        super.stop();
    }


    private static class myTask implements Runnable{
        private  String filePath;

        private  String offsetFile;

        private  Long interval;
        private Long osLong=0L;
        private  String charset;
        private ChannelProcessor channelProcessor;
        private  RandomAccessFile randomAccessFile;
        private boolean flag=true;
        private File offsetfile;

        public myTask(String filePath, String offsetFile, Long interval, String charset, ChannelProcessor channelProcessor) {
            this.filePath = filePath;
            this.offsetFile = offsetFile;
            this.interval = interval;
            this.charset = charset;
            this.channelProcessor = channelProcessor;

             offsetfile = new File(offsetFile);
            //判断偏移量文件是否存在
            try {
                if (!offsetfile.exists()){
                    offsetfile.createNewFile();
                }
            } catch (IOException e) {
                logger.error("offsetFile doesn't exist",e);

            }

            //读取文件里的内容
            try {
                String content = FileUtils.readFileToString(offsetfile);
                //偏移量存在 读取偏移量 不存在从头读
                if(StringUtils.isNotEmpty(content)){
                    osLong = Long.parseLong(content);

                }
                //只读模式
                 randomAccessFile = new RandomAccessFile(filePath,"r");
                randomAccessFile.seek(osLong);

            } catch (IOException e) {
                logger.error("read offset failed",e);

            }


        }


        public void run() {
            //定期读取文件，是否有新内容
            while(flag) {
                try {
                    String line = randomAccessFile.readLine();
                    if (StringUtils.isNotEmpty(line)) {
                        //然后将数据封装到Event中
                        Event event = EventBuilder.withBody(line, Charset.forName(charset));
                        //将Event发送给Channel
                        channelProcessor.processEvent(event);
                        //获取最新的偏移量
                        osLong = randomAccessFile.getFilePointer();
                        //存储偏移量
                        FileUtils.writeStringToFile(offsetfile,osLong.toString());

                    } else {
                        //日志文件中没有新内容
                        //休息一会
                        Thread.sleep(interval);
                    }


                } catch (FileNotFoundException e) {
                    logger.error("filePath doesn't exist", e);
                } catch (IOException e) {
                    logger.error("read line failed", e);
                } catch (InterruptedException e) {
                    logger.error("InterruptedException", e);
                }

            }
        }
        public void setflag(boolean flag){
            this.flag=flag;

        }

    }

}
