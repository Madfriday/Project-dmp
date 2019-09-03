package tags

import Utils.Getredis
import org.apache.spark.graphx.{Edge, Graph}
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.{Dataset, SQLContext}
import org.apache.spark.{SparkConf, SparkContext}

import scala.collection.mutable.ListBuffer

object TagsCreate {
  def main(args: Array[String]): Unit = {
    val conf = new SparkConf().setAppName(s"${this.getClass.getSimpleName}").setMaster("local[2]")

    conf.set("spark.serialzer","org.apache.spark.serializer.KryoSerializer")

    val sc = new SparkContext(conf)

    val broadcast = sc.textFile("d:/w.jar/app.txt").map(x =>{
      val con = x.split(",")
      val id = con(0)
      val name = con(1)
      (id,name)
    }).collect().toMap

    val qc = new SQLContext(sc)

    import qc.implicits._

    val lines = qc.read.parquet("d:/w.jar/allforyou/")

    val re = lines.where(
      """
        |imei != "" or imeimd5 != "" or
        |imeisha1 != "" or idfa != "" or idfamd5 != "" or
        |idfasha1 != "" or mac != "" or macmd5 != "" or
        |macsha1 != "" or androidid != "" or androididmd5 != ""
        |or androididsha1 !=""
        |""".stripMargin)

      val r = re.mapPartitions(x => {
        val jedis = Getredis.jedi()

        x.map(x => {
          val ads = TagsAD.tags(x)
          val apps = Tags4App.tags(x, broadcast.values)
          val alluser = Alluserid.getallUser(x)
          val bus = Tags4Business.tags(x, jedis)

          (alluser(0), (ads++apps ++bus).toList)
        })
      }).rdd.reduceByKey((x,y) =>{
        (x++y).groupBy(_._1).mapValues(_.foldLeft(0)(_+_._2)).toList
      })

        r.saveAsTextFile("d:/w.jar/app/")








  }
}
