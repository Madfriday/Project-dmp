package tags

import org.apache.spark.sql.Row

import scala.collection.mutable.ListBuffer

object Alluserid {
  def getallUser(v : Row) : ListBuffer[String] = {
    val userid = new collection.mutable.ListBuffer[String]()
    if(v.getAs[String]("imei").nonEmpty){
      userid.append("" + v.getAs[String]("imei"))
    }
    else if(v.getAs[String]("idfa").nonEmpty){
      userid.append("" + v.getAs[String]("idfa"))
    }
    else if(v.getAs[String]("mac").nonEmpty){
      userid.append(v.getAs[String]("mac"))
    }
    userid

  }

}
