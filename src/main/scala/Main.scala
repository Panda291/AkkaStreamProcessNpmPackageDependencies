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
  val ObjectsThrottle = Flow[NpmPackage].throttle(1, 3.second)

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
            Flow[Dependency].fold(AccumulatedDependencyCount())((acc, n) => {
              //              println(n)
              if (acc.map.contains(n.packageName)) {
                if (acc.map(n.packageName).contains(n.packageVersion)) {
                  val (run, dev) = acc.map(n.packageName)(n.packageVersion)
                  if (n.dependencyType == "runtime") {
                    acc.map(n.packageName)(n.packageVersion) = (run + 1, dev)
                  } else {
                    acc.map(n.packageName)(n.packageVersion) = (run, dev + 1)
                  }
                } else {
                  if (n.dependencyType == "runtime") {
                    acc.map(n.packageName) += (n.packageVersion -> (1, 0))
                  } else {
                    acc.map(n.packageName) += (n.packageVersion -> (0, 1))
                  }
                }
              } else {
                if (n.dependencyType == "runtime") {
                  acc.map += (n.packageName -> mutable.Map(n.packageVersion -> (1, 0)))
                } else {
                  acc.map += (n.packageName -> mutable.Map(n.packageVersion -> (0, 1)))
                }
              }
              val temp = acc.copy() // the instance of AccumulatedDependencyCount got defined when the value flowCountDependencies was instantiated
              acc.empty()           // this meant that every filter flow on both balance flows were accessing and accumulating on the SAME object
              temp                  // i resolved this by clearing out the accumulator and returning a copy instead
            })

          val debugPrinter: Flow[Dependency, Dependency, NotUsed] =
            Flow[Dependency].map(d => {
              println(d)
              d
            })

          broadcast ~> filterDependencies ~> flowCountDependencies.async ~> zipValues.in0
          broadcast ~> filterDevDependencies ~> flowCountDependencies.async ~> zipValues.in1

          FlowShape(broadcast.in, zipValues.out)
        }
      )

      val dispatchDependencyCheck = builder.add(Balance[Version](2))
      val mergeDependencies = builder.add(Merge[AccumulatedDependencyCount](2))

      val flowMapDependencies: Flow[Version, Dependency, NotUsed] =
        Flow[Version].mapConcat(_.dependencyList)

      val flowPairToSingle: Flow[(AccumulatedDependencyCount, AccumulatedDependencyCount), AccumulatedDependencyCount, NotUsed] =
        Flow[(AccumulatedDependencyCount, AccumulatedDependencyCount)].map(accs => {
          //          println(accs._1.map)
          //          println(accs._2.map)
          for ((k, v) <- accs._2.map) { // for each package in map 2
            if (!accs._1.map.contains(k)) { // if it does not exist in map 1
              accs._1.map += (k -> v) // add it
            } else { // if not:
              for ((k2, v2) <- v) { // for each version for a package in map 2
                if (!accs._1.map(k).contains(k2)) { // if it does not exist in map 1
                  accs._1.map(k) += (k2 -> v2) // add it
                } else { // if not:
                  val (discard, dev) = v2
                  val (run, discard2) = accs._1.map(k)(k2)
                  accs._1.map(k)(k2) = (run, dev)
                }
              }
            }
          }
          accs._1
        })

      val debugPrinter: Flow[(AccumulatedDependencyCount, AccumulatedDependencyCount), (AccumulatedDependencyCount, AccumulatedDependencyCount), NotUsed] =
        Flow[(AccumulatedDependencyCount, AccumulatedDependencyCount)].map((a) => {
          println(a._1.map)
          println(a._2.map)
          a
        })

      dispatchDependencyCheck.out(0) ~> flowMapDependencies.async ~> flowFilterDependencies.async ~> flowPairToSingle.async ~> mergeDependencies.in(0)
      dispatchDependencyCheck.out(1) ~> flowMapDependencies.async ~> flowFilterDependencies.async ~> flowPairToSingle.async ~> mergeDependencies.in(1)

      FlowShape(dispatchDependencyCheck.in, mergeDependencies.out)
    }

  )

  val flowFoldAccumulatedDependencyCounts: Flow[AccumulatedDependencyCount, AccumulatedDependencyCount, NotUsed] =
    Flow[AccumulatedDependencyCount].fold(AccumulatedDependencyCount())((acc, n) => {
      for ((k, v) <- n.map) {
        if(acc.map.contains(k)) {
          for ((k2, v2) <- v) {
            if (acc.map(k).contains(k2)) {

            } else {
              acc.map(k) += (k2 -> v2)
            }
          }
        } else {
          acc.map += (k -> v)
        }
      }
      acc
    })

  val sink: Sink[AccumulatedDependencyCount, Future[Done]] = Sink.foreach(adc => {
    for ((k, v) <- adc.map) {
      println("Analysing " + k)
      for ((v, (r, d)) <- v) {
        println("version: " + v + ", Dependencies: " + r + ", DevDependencies: " + d)
      }
    }
  })

  val runnableGraph: RunnableGraph[Future[IOResult]] =
      source
      .via(flowUnzipAndCreateObjects)
//    Source(List(NpmPackage("aconite")))
      .via(ObjectsBuffer)
      .via(ObjectsThrottle)
      .via(flowFetchDependencies)
      .via(flowMapVersions)
      .buffer(12, OverflowStrategy.backpressure)
      .via(flowDependencies)
      .via(flowFoldAccumulatedDependencyCounts)
      .to(sink)
  runnableGraph.run()
}
