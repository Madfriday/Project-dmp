package Utils

import scala.collection.mutable

trait TagsUtils {
  def tags(v : Any*):mutable.HashMap[String,Int]

}
