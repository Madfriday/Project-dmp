package tags

import Utils.TagsUtils
import org.apache.commons.lang.StringUtils
import org.apache.spark.sql.Row

import scala.collection.mutable

object Tags4App extends  TagsUtils{
  override def tags(v: Any*): mutable.HashMap[String, Int] = {
    var map = new mutable.HashMap[String,Int]()
    val row = v(0).asInstanceOf[Row]
//    val appdict = v(1).asInstanceOf[Map[String,String]]
    val id = row.getAs[String]("appid")
    val name = row.getAs[String]("appname")
//    if(StringUtils.isEmpty(name)){
//      appdict.contains(id) match{
//        case true =>
//      }
    map += "APP" + name-> 1
    }


}
