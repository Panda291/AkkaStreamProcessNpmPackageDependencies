import ujson.Value

case class Version(version: String, jsonObject: Value) {
  val packageName: String = jsonObject("name").str
  var dependencyList: List[Dependency] = List()
  try {
    for ((k, v) <- jsonObject("dependencies").obj.toList) {
      dependencyList = Dependency(packageName, v.str, "runtime") +: dependencyList
    }
  } catch {
    case e: Exception =>
  }

  try {
    for ((k, v) <- jsonObject("devDependencies").obj.toList) {
      dependencyList = Dependency(packageName, v.str, "dev") +: dependencyList
    }
  } catch {
    case e: Exception =>
  }
}
