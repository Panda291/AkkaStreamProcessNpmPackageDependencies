import scala.collection.mutable

case class AccumulatedDependencyCount() {
  var map: mutable.Map[String, mutable.Map[String, (Int, Int)]] = mutable.Map()

  def empty(): Unit = {
    map = mutable.Map()
  }

  def copy(): AccumulatedDependencyCount = {
    var temp = AccumulatedDependencyCount()
    temp.map = map
    temp
  }
}
