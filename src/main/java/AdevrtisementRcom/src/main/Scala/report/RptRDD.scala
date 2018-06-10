package report

import Config.Confighelper
import beans.Logbean
import com.alibaba.fastjson.JSON
import org.apache.spark._
import org.slf4j.LoggerFactory


/**
  * User: Russell
  * Date: 2018-06-10
  * Time: 10:46
  */
object RptRDD {
  private val logger= LoggerFactory.getLogger(this.getClass)
  def main(args: Array[String]): Unit = {
    val conf = new SparkConf()
      .setMaster("local[*]")
      .setAppName("日志数据统计")
      .set("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
      .set("spark.streaming.stopGracefullyOnShutdown", "true")

    val sc = new SparkContext(conf)

    val logdata = sc.textFile(Confighelper.log_path)

    val basedata = logdata.map(line => {
      var logbean:Logbean= null
      try {
         logbean = JSON.parseObject(line, classOf[Logbean])
      } catch {
        case e:Exception =>logger.error("json error=>"+line)
      }
    logbean
    })

    //统计 pv uv
    val pv=basedata.count()
    val filtered = basedata.filter(t=>t!=null&&t.openid!=null)
    val uv=filtered.map(t=>t.openid).distinct().count()

    println("pv="+pv,"uv="+uv)
    //统计省市分布

     val areaDist=filtered.map(t=>{
       ((t.openid,t.province,t.city),1)
     }).reduceByKey(_+_)

    areaDist.foreach(println)


    //各个分类（手机、书）商品页面被浏览次数最多的TON2

    /*val cagSort=basedata.filter(t=>t.openid!=null&&t.cid!=null&&t.event_type.equals("1")).map(t=>{

     val cag= t.cid match {
        case "1"=>"图书"
        case "2"=>"服装"
        case "3"=>"家具"
        case "4"=>"手机"
      }

      ((cag,t.pid),1)
    }).reduceByKey(_+_)
    //分组排序
   val res= cagSort.groupBy(t=>t._1._1).mapValues(it=>{
    /*  it.toList.sortBy( -_._2)*/


    })

    cagSort.foreach(println)*/


    sc.stop()
  }


}
