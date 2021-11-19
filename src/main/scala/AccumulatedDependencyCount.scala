import scala.collection.mutable

case class AccumulatedDependencyCount() {
  var map: mutable.Map[String, mutable.Map[String, (Int, Int)]] = mutable.Map()
}
