case class DependencyCount (
  var packageName: String = "",
  var version: String = "",
  var dependencies: Int = 0,
  var devDependencies: Int  = 0
)
