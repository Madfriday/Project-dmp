package Tools

import Utils.{Extract4Busness, Getredis}
import ch.hsr.geohash.GeoHash
import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.sql.SQLContext

object Extract4dict {
  def main(args: Array[String]): Unit = {
    val conf = new SparkConf().setAppName(s"${this.getClass.getSimpleName}").setMaster("local[2]")
    //
    conf.set("spark.serializer","org.apache.spark.serializer.KryoSerializer")
    //
    val sc = new SparkContext(conf)
    //
    val qc = new SQLContext(sc)
    import qc.implicits._
    //
    val re = qc.read.parquet("d:/w.jar/allforyou").select("lat","long").where("lat > 3 and lat < 54  and long > 73 and long < 136").distinct().foreachPartition(x =>{
      val jedis = Getredis.jedi()

      x.foreach(x => {
        val lat = x.getAs[String]("lat")
        val long = x.getAs[String]("long")
        val geohashCode = GeoHash.withCharacterPrecision(lat.toDouble, long.toDouble, 8).toBase32
        val business = Extract4Busness.getBuss(lat + "," + long)
        println(business)
        println(geohashCode)
        if (null != business) {
       jedis.set(geohashCode, "" + business)
        }
      })
      jedis.close()
    })





    //


  }

}
