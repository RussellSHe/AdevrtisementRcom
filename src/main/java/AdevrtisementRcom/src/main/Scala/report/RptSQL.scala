package report

import Config.Confighelper
import org.apache.spark._
import org.apache.spark.sql._

/**
  * User: Russell
  * Date: 2018-06-10
  * Time: 13:44
  */
object RptSQL {
  def main(args: Array[String]): Unit = {
    val conf = new SparkConf()
      .setMaster("local[*]")
      .setAppName("日志数据统计")
     .set("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
      .set("spark.streaming.stopGracefullyOnShutdown", "true")

    val session = SparkSession.builder()
      .config(conf)
      .getOrCreate()

    val frame = session.read.json(Confighelper.log_path)
    //pv uv
    frame.createTempView("tb_pvuv")

    val res = session.sql("select now(), count(1) pv, count(distinct(openid)) uv from tb_pvuv where openid is not null")
    res.show()

    //省市分布
    session.sql(" select t.openid, t.province from (select distinct(openid,province) t from tb_pvuv where openid is not null) ")
        .show(10)


    session.stop()

  }
}
