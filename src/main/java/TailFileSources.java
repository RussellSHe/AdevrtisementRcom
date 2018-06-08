
import org.apache.commons.io.FileUtils;
import org.apache.commons.lang.StringUtils;
import org.apache.flume.Context;
import org.apache.flume.Event;
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
 * User: Russell
 * Date: 2018-06-07
 * Time: 21:19
 */
public class TailFileSources extends AbstractSource implements Configurable,EventDrivenSource {
    private static final Logger logger = LoggerFactory.getLogger(TailFileSources.class);
    private String filePath;
    private String offsetPath;
    private Long interval;
    private String charSet;
    private ExecutorService executor;
    private   fileRunner fileRunner;
    private ChannelProcessor ChProcessor;


    public void configure(Context context) {
         filePath = context.getString("filePath");
         offsetPath = context.getString("offsetPath");
         interval = context.getLong("interval",1000L);
         charSet = context.getString("charSet","utf-8");

    }

    @Override
    public synchronized void start() {
         executor = Executors.newSingleThreadExecutor();
        ChProcessor = getChannelProcessor();
         fileRunner = new fileRunner(filePath,offsetPath,interval,charSet,ChProcessor);
        executor.execute(fileRunner);

        super.start();
    }

    @Override
    public synchronized void stop() {
        //关闭线程
        fileRunner.setFlag(false);
        //关闭线程池
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

        super.stop();
    }

    private static class fileRunner implements Runnable{
        private String filePath;
        private String offsetPath;
        private Long interval;
        private String charSet;
        private ChannelProcessor ChProcessor;
        private Long offset=0L;
        private  boolean flag=true;
        private File osFile;
        private RandomAccessFile raf;

        public fileRunner(String filePath, String offsetPath, Long interval, String charSet, ChannelProcessor chProcessor) {
            this.filePath = filePath;
            this.offsetPath = offsetPath;
            this.interval = interval;
            this.charSet = charSet;
            this.ChProcessor = chProcessor;
            //判断偏移量文件是否存在
             osFile = new File(offsetPath);
            try {
                if(!osFile.exists()){
                    osFile.createNewFile();
                }

                String content = FileUtils.readFileToString(osFile);
                //判断是否有内容
                if(StringUtils.isNotEmpty(content)){
                     offset = Long.parseLong(content);
                }

                 raf = new RandomAccessFile(filePath,"r");
                raf.seek(offset);


            } catch (IOException e) {
                logger.error("offsetFile doesn't exist",e);
            }

            //读取偏移量
        }

        public void run() {
          //定期读取日志文件 如果有新内容 将内容封装为evevt对象
            while(flag) {
                try {
                    String line = raf.readLine();
                    if(StringUtils.isNotEmpty(line)){
                        //randomaccessfile文件中有中文出现乱码
                        line= new String(line.getBytes("iso-8859-1"), charSet);
                        Event event = EventBuilder.withBody(line.getBytes());
                        //将Event发送给channel
                        ChProcessor.processEvent(event);
                        //获取新的偏移量
                       offset = raf.getFilePointer();
                        //存储偏移量
                        FileUtils.writeStringToFile(osFile,offset+"");

                    }else {
                        //没有 睡一会儿
                        Thread.sleep(interval);
                    }

                } catch (FileNotFoundException e) {
                    logger.error("filePath doesn't exist", e);
                } catch (IOException e) {
                    logger.error("read file failed", e);
                } catch (InterruptedException e) {
                    logger.error("InterruptedException", e);
                }
            }

        }
        public  void setFlag(boolean  flag){
            this.flag=flag;
        }
    }

}
