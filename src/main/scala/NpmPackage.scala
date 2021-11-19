import ujson.Value

case class NpmPackage(name: String) {
  var versionList: List[Version] = List()
  def fetchDependencies(): NpmPackage = {
    val r = requests.get("https://registry.npmjs.org/" + name)
    if (r.statusCode == 200) {
      val data = ujson.read(r.text())
      for ((k,v) <- data("versions").obj.toList) {
//        println(k, v)
        versionList = Version(k, v) +: versionList
      }
    }
    this
  }
}