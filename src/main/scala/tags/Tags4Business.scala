package tags

import Utils.{Getredis, TagsUtils}
import ch.hsr.geohash.GeoHash
import org.apache.commons.lang.StringUtils
import org.apache.spark.sql.Row
import redis.clients.jedis.Jedis

import scala.collection.mutable


object Tags4Business extends TagsUtils{
  override def tags(v: Any*): mutable.HashMap[String, Int] = {
    val map = mutable.HashMap[String,Int]()
    val row = v(0).asInstanceOf[Row]
    val lat = row.getAs[String]("lat")
    val long = row.getAs[String]("long")
    val jeids = v(1).asInstanceOf[Jedis]


    if(StringUtils.isNotEmpty(lat) && StringUtils.isNotEmpty(long)){
      if(lat.toDouble > 3 & lat.toDouble < 54 & long.toDouble > 73 & long.toDouble < 136){


      val geoHashCode = GeoHash.withCharacterPrecision(lat.toDouble,long.toDouble,8).toBase32

      val code = jeids.get(geoHashCode)
//        println(code)
        if(StringUtils.isNotEmpty(code)) {


          code.split(",").foreach(b => {
            map += "BS" + code -> 1
          })
        }


      }
    }

    map
  }
}
