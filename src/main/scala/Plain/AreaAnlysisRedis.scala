package Plain

import Utils.Getredis
import org.apache.spark.{SparkConf, SparkContext}
import redis.clients.jedis.JedisPool

import scala.collection.mutable.ListBuffer



object AreaAnlysisRedis {
  def main(args: Array[String]): Unit = {
    val conf = new SparkConf().setAppName(s"${this.getClass.getSimpleName}").setMaster("local[2]")
    //
    val sc = new SparkContext(conf)
    //
    val lines = sc.textFile("d:/w.jar/logs.log")
    //
    val line = lines.map(t => {
      val x = t.split(",", -1)
      x
    }).filter(_.length >= 85).filter(x => {
      !x(15).isEmpty || !x(16).isEmpty
    }).foreachPartition(x =>{
      val jed = Getredis.jedi()
     x.foreach(x =>{
       var appname = x(16)
       if("".equals(appname)){
         appname = jed.get(x(15))
       }
       val effrequest = if (x(8).toInt == 1 && x(35).toInt >= 2) 1 else 0
       val adrequest = if (x(8).toInt == 1 && x(35).toInt >= 3) 1 else 0
       val bidnum = if (x(30) == "1" && x(31) == "1" && x(39) == "1" && x(2) != 0) 1 else 0
       val sucessnum = if (x(30) == "1" && x(31) == "1" && x(42) == "1") 1 else 0
       ((appname),(effrequest,adrequest,bidnum,sucessnum))
     })
      jed.close()
    })


  }
}
