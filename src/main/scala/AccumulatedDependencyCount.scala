import scala.collection.mutable

case class AccumulatedDependencyCount() {
  var map: mutable.Map[String, mutable.Map[String, (Int, Int)]] = mutable.Map()

  def addDep(dependencyCount: DependencyCount): AccumulatedDependencyCount = {
    try {
      if (map.contains(dependencyCount.packageName)) {
        map(dependencyCount.packageName) += (dependencyCount.version -> (dependencyCount.dependencies, dependencyCount.devDependencies))
      } else {
        map += (dependencyCount.packageName -> mutable.Map(dependencyCount.version -> (dependencyCount.dependencies, dependencyCount.devDependencies)))
      }
    } catch {
      case e: Exception => println(e)
    }
    this
  }
}
