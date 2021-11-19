import akka.actor.ActorSystem
import akka.stream._
import akka.stream.scaladsl.{Balance, Broadcast, Compression, FileIO, Flow, GraphDSL, Merge, RunnableGraph, Sink, Source, Zip}
import akka.util.ByteString
import akka.{Done, NotUsed}

import java.nio.file.{Path, Paths}
import scala.+:
import scala.collection.mutable
import scala.concurrent.duration.DurationInt
import scala.concurrent.{ExecutionContextExecutor, Future}

object Main extends App {
  implicit val actorSystem: ActorSystem = ActorSystem("Main")
  implicit val dispatcher: ExecutionContextExecutor = actorSystem.dispatcher
  implicit val actorMaterializer: ActorMaterializer = ActorMaterializer()

  val resourcesFolder: String = "src/main/resources"
  val pathGzFile: Path = Paths.get(s"$resourcesFolder/packages.txt.gz")

  val source: Source[ByteString, Future[IOResult]] = FileIO.fromPath(pathGzFile)

  val flowUnzip: Flow[ByteString, ByteString, NotUsed] = Compression.gunzip()
  val flowStringify: Flow[ByteString, String, NotUsed] = Flow[ByteString].map(_.utf8String)
  val flowSplitLines: Flow[String, String, NotUsed] = Flow[String].mapConcat(_.split("\n").toList)
  val flowObjectify: Flow[String, NpmPackage, NotUsed] = Flow[String].map(NpmPackage)

  val flowUnzipAndCreateObjects: Flow[ByteString, NpmPackage, NotUsed] = flowUnzip
    .via(flowStringify)
    .via(flowSplitLines)
    .via(flowObjectify)

  val ObjectsBuffer = Flow[NpmPackage].buffer(10, OverflowStrategy.backpressure)
  val ObjectsThrottle = Flow[NpmPackage].throttle(1, 1.second) // TODO: change back to 3.seconds !!!!

  val flowFetchDependencies: Flow[NpmPackage, NpmPackage, NotUsed] =
    Flow[NpmPackage].map(_.fetchDependencies())

  val flowMapVersions: Flow[NpmPackage, Version, NotUsed] =
    Flow[NpmPackage].mapConcat(_.versionList)

  val flowDependencies: Graph[FlowShape[Version, AccumulatedDependencyCount], NotUsed] = Flow.fromGraph(
    GraphDSL.create() { implicit builder =>
      import GraphDSL.Implicits._

      val flowFilterDependencies: Graph[FlowShape[Dependency, (AccumulatedDependencyCount, AccumulatedDependencyCount)], NotUsed] = Flow.fromGraph(
        GraphDSL.create() { implicit builder =>
          val broadcast = builder.add(Broadcast[Dependency](2))
          val zipValues = builder.add(Zip[AccumulatedDependencyCount, AccumulatedDependencyCount])

          val filterDependencies: Flow[Dependency, Dependency, NotUsed] =
            Flow[Dependency].filter(_.dependencyType == "runtime")
          val filterDevDependencies: Flow[Dependency, Dependency, NotUsed] =
            Flow[Dependency].filter(_.dependencyType == "dev")

          val flowCountDependencies: Flow[Dependency, AccumulatedDependencyCount, NotUsed] =
            Flow[Dependency].fold(new AccumulatedDependencyCount)((acc, n) => {
              if (acc.map.contains(n.packageName)) {
                if (acc.map(n.packageName).contains(n.version)) {
                  val (run, dev) = acc.map(n.packageName)(n.version)
                  acc.map(n.packageName)(n.version) = (run + 1, dev)
                } else {
                  acc.map(n.packageName) += (n.version -> (1, 0))
                }
              } else {
                acc.map += (n.packageName -> mutable.Map(n.version -> (1, 0)))
              }
              acc
            })

          val debugPrinter: Flow[AccumulatedDependencyCount, AccumulatedDependencyCount, NotUsed] =
            Flow[AccumulatedDependencyCount].map(d => {
              println(d.map)
              d
            })

          broadcast ~> filterDependencies ~> flowCountDependencies ~> debugPrinter ~> zipValues.in0
          broadcast ~> filterDevDependencies ~> flowCountDependencies ~> debugPrinter ~> zipValues.in1

          FlowShape(broadcast.in, zipValues.out)
        }
      )

      val dispatchDependencyCheck = builder.add(Balance[Version](2))
      val mergeDependencies = builder.add(Merge[AccumulatedDependencyCount](2))

      val flowMapDependencies: Flow[Version, Dependency, NotUsed] =
        Flow[Version].flatMapConcat(version => Source(version.dependencyList))

      val flowPairToSingle: Flow[(AccumulatedDependencyCount, AccumulatedDependencyCount), AccumulatedDependencyCount, NotUsed] =
        Flow[(AccumulatedDependencyCount, AccumulatedDependencyCount)].map(accs => {
          for ((k, v) <- accs._2.map) {             // for each package in map 2
            if(!accs._1.map.contains(k)) {          // if it does not exist in map 1
              accs._1.map += (k -> v)                      // add it
            } else {                                // if not:
              for ((k2, v2) <- v) {                 // for each version for a package in map 2
                if (!accs._1.map(k).contains(k2)) { // if it does not exist in map 1
                  accs._1.map(k) += (k2 -> v2)              // add it
                } else {                            // if not:
                  val (dev, discard) = v2
                  val (run, discard2) = accs._1.map(k)(k2)
                  accs._1.map(k)(k2) = (run, dev)
                }
              }
            }
          }
          accs._1
        })

  val debugPrinter: Flow[Dependency, Dependency, NotUsed] =
    Flow[Dependency].map(d => {
      println(d.version, d.packageName, d.dependencyType)
      d
    })

  dispatchDependencyCheck.out(0) ~> flowMapDependencies ~> flowFilterDependencies.async ~> flowPairToSingle ~> mergeDependencies.in(0)
  dispatchDependencyCheck.out(1) ~> flowMapDependencies ~> flowFilterDependencies.async ~> flowPairToSingle ~> mergeDependencies.in(1)

  FlowShape(dispatchDependencyCheck.in, mergeDependencies.out)
}

)

val flowCollectDependencies: Flow[DependencyCount, mutable.Map[String, List[(String, Int, Int)]], NotUsed] =
  Flow[DependencyCount].fold(mutable.Map[String, List[(String, Int, Int)]]())((acc, n) => {
    println(acc, n.packageName)
    if (acc.contains(n.packageName)) {
      acc(n.packageName) = (n.version, n.dependencies, n.devDependencies) +: acc(n.packageName)
      acc
    } else {
      acc += (n.packageName -> List((n.version, n.dependencies, n.devDependencies)))
      acc
    }
  })

val sink: Sink[AccumulatedDependencyCount, Future[Done]] = Sink.foreach(adc => {
  for ((k, v) <- adc.map) {
    println("Analysing " + k)
    for ((v, (r, d)) <- v) {
      println("version: " + v + ", Dependencies: " + r + ", DevDependencies: " + d)
    }
  }
})
val runnableGraph: RunnableGraph[Future[IOResult]] = source
  .via(flowUnzipAndCreateObjects)
  .via(ObjectsBuffer)
  .via(ObjectsThrottle)
  .via(flowFetchDependencies)
  .via(flowMapVersions)
  .buffer(12, OverflowStrategy.backpressure)
  .via(flowDependencies)
//      .via(flowCollectDependencies)
      .to(sink)
//  .to(Sink.foreach(println)
runnableGraph.run()
}
