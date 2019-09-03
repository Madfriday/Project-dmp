package tags

import Utils.TagsUtils
import org.apache.commons.lang.StringUtils
import org.apache.spark.sql.Row

import scala.collection.mutable

object TagsAD extends TagsUtils{
  override def tags(v: Any*): mutable.HashMap[String, Int] = {
    var map = new mutable.HashMap[String,Int]()
    val row = v(0).asInstanceOf[Row]
    val adty = row.getAs[Int]("adspacetype")
    val adn = row.getAs[String]("adspacetypename")
    if(adty > 9) {map += "LC" + adty -> 1}
    else if(adty > 1) map += "LC0" + adty ->1
    if(StringUtils.isNotEmpty(adn)){
      map += "LN" +adn -> 1
    }
    map
  }
}
