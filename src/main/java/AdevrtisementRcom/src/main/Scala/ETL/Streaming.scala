package ETL

import java.lang

import Config.Confighelper
import beans.{Logbean, Logbean2}
import com.alibaba.fastjson.JSON
import org.apache.kafka.clients.consumer.ConsumerRecord
import org.apache.kafka.common.serialization.StringDeserializer
import org.apache.spark._
import org.apache.spark.rdd.RDD
import org.apache.spark.sql._
import org.apache.spark.streaming._
import org.apache.spark.streaming.dstream.InputDStream
import org.apache.spark.streaming.kafka010._
import org.slf4j.{Logger, LoggerFactory}
import utils.{CaculateKpi, Jpools}

/**
  * User: Russell
  * Date: 2018-06-11
  * Time: 18:07
  */
object Streaming {
  private val logger: Logger = LoggerFactory.getLogger(Streaming.getClass)

  def main(args: Array[String]): Unit = {
    System.setProperty("HADOOP_USER_NAME", "root")

    val conf = new SparkConf()
      //.setMaster("local[*]")
      .setAppName("日志数据统计")
      .set("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
      .set("spark.streaming.stopGracefullyOnShutdown", "true")
      .set("spark.streaming.kafka.maxRatePerPartition", "1000")
    //指定是否以本地模式运行
    val islocal = args(0).toBoolean

    if (islocal) conf.setMaster("local[*]")
    //一个JVM里只能有一个SparkContext  先创建StreamingContext 再创建
    val ssc = new StreamingContext(conf, Seconds(2))
    //再创建 session 获取上步创建好的sparkcontext
    val session = SparkSession.builder().config(conf).getOrCreate()

    //kafka参数配置
    val kafkaprams = Map[String, Object](
      "bootstrap.servers" -> Confighelper.brokers,
      "key.deserializer" -> classOf[StringDeserializer],
      "value.deserializer" -> classOf[StringDeserializer],
      "group.id" -> Confighelper.groupid,
      "auto.offset.reset" -> "earliest",
      // 是否可以自动提交偏移量   自定义
      "enable.auto.commit" -> (false: lang.Boolean)
    )
    //加载库中初始偏移量
    /* val offsets = mutable.HashMap[TopicPartition, Long]()

     val jedis = Jpools.getjedis
     val part_offset: util.Map[String, String] = jedis.hgetAll(Confighelper.groupid+"-"+Confighelper.topics)
     import scala.collection.JavaConversions._

     for(i<-part_offset){

      offsets+=( new TopicPartition(Confighelper.topics,i._1.toInt)->i._2.toLong)
     }
     jedis.close()
 */
    //直连KAFKA 不能使用Dstream转换API
    val stream: InputDStream[ConsumerRecord[String, String]] = KafkaUtils.createDirectStream[String, String](ssc,
      LocationStrategies.PreferConsistent,
      ConsumerStrategies.Subscribe[String, String](Confighelper.topicarr, kafkaprams))
    //想要获取偏移量 就必须使用第一手Dstream kafkardd

    stream.foreachRDD(rdd => {

      //获取当前批次rdd的的偏移量
      if (!rdd.isEmpty()) {
        //只有kafkardd继承了OffsetRange特质，才能将其转换为HasoffsetRange获取到偏移量
        val offsetRanges: Array[OffsetRange] = rdd.asInstanceOf[HasOffsetRanges].offsetRanges

        val filtered: RDD[Logbean2] = rdd.map(_.value()).map(line => {

          var logbean2: Logbean2 = null
          try {
            logbean2 = JSON.parseObject(line, classOf[Logbean2])

          } catch {
            case e: Exception => logger.error("json error=>" + line)
          }
          logbean2

        }).filter(t => t != null)

        import session.implicits._
        //转换成dataframe

        val df = filtered.toDF()
        //转为parquet文件存到HDFS上
       //df.write.mode(SaveMode.Append).json(args(1).toString)

        //df.registerTempTable("tb_log")
        /**
          * 统计总销售金额；从今天凌晨到目前的销售金额（Redis）
          */
        // df.select("openuid","time").filter("openuid is not null and time>='2018-05-20 00:00:00' and time<=current_date()")

          CaculateKpi.getTotalMoney(session,df)
          CaculateKpi.getTodayTotalMoney(session,df)

        /**
          * 按照分类进行累加销售金额（Redis）
          */
       // CaculateKpi.getCagTotalMoney(session,df)

        /**
          * 按照省份进行累加销售金额（Redis）
          */
        //CaculateKpi.getProTotalMoney(session,df)
          //释放存放再内存的filtered
        //filtered.unpersist(false)
          offsetRanges.foreach(t=>println(t))
        //手动提交offset 手动提交偏移量的操作,储存在broker下一个主题中 __consumer_offsets driver端执行
        //  stream.asInstanceOf[CanCommitOffsets].commitAsync(offsetRanges)
      }
    })


    ssc.start()
    ssc.awaitTermination()


  }

}
