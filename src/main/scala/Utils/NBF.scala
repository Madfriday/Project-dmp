package Utils

object NBF {
  def toInt(value : String) = {
    try {
      value.toInt
    } catch {
      case _ :Exception => 0
      }
    }

  def toDouble(value : String) = {

    try {
      value.toDouble
    } catch {
      case _: Exception =>0.0
    }
  }

}
