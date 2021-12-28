import akka.actor.ActorSystem
import akka.stream._
import akka.stream.scaladsl.{Balance, Broadcast, Compression, FileIO, Flow, GraphDSL, Merge, RunnableGraph, Sink, Source, Zip}
import akka.util.ByteString
import akka.{Done, NotUsed}

import java.nio.file.{Path, Paths}
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

//  val ObjectsBuffer = Flow[NpmPackage].buffer(10, OverflowStrategy.backpressure)
  val ObjectsThrottle = Flow[NpmPackage].throttle(1, 3.second)

  val flowFetchDependencies: Flow[NpmPackage, NpmPackage, NotUsed] =
    Flow[NpmPackage].map(_.fetchDependencies())

  val flowMapVersions: Flow[NpmPackage, Version, NotUsed] =
    Flow[NpmPackage].mapConcat(_.versionList)

  val flowDependencies: Graph[FlowShape[Version, DependencyCount], NotUsed] = Flow.fromGraph(
    GraphDSL.create() { implicit builder =>
      import GraphDSL.Implicits._

      val flowFilterDependencies: Graph[FlowShape[Version, (DependencyCount, DependencyCount)], NotUsed] = Flow.fromGraph(
        GraphDSL.create() { implicit builder =>
          val broadcast = builder.add(Broadcast[Version](2))
          val zipValues = builder.add(Zip[DependencyCount, DependencyCount])

          val filterDependencies: Flow[Version, DependencyCount, NotUsed] =
            Flow[Version].map({ version =>
              DependencyCount(version.packageName, version.version, dependencies = version.dependencyList.count(_.dependencyType == "runtime"))
            })
          val filterDevDependencies: Flow[Version, DependencyCount, NotUsed] =
            Flow[Version].map({ version =>
              DependencyCount(version.packageName, version.version, devDependencies = version.dependencyList.count(_.dependencyType == "dev"))
            })

          broadcast ~> filterDependencies.async ~> zipValues.in0
          broadcast ~> filterDevDependencies.async ~> zipValues.in1

          FlowShape(broadcast.in, zipValues.out)
        }
      )

      val dispatchDependencyCheck = builder.add(Balance[Version](2))
      val mergeDependencies = builder.add(Merge[DependencyCount](2))

      val flowPairToSingle: Flow[(DependencyCount, DependencyCount), DependencyCount, NotUsed] =
        Flow[(DependencyCount, DependencyCount)].map(counts => {
          counts._1.devDependencies = counts._2.devDependencies
          counts._1
        })

      dispatchDependencyCheck.out(0) ~> flowFilterDependencies.async ~> flowPairToSingle.async ~> mergeDependencies.in(0)
      dispatchDependencyCheck.out(1) ~> flowFilterDependencies.async ~> flowPairToSingle.async ~> mergeDependencies.in(1)

      FlowShape(dispatchDependencyCheck.in, mergeDependencies.out)
    }

  )

  val flowAccumulateDependencyCounts: Flow[DependencyCount, AccumulatedDependencyCount, NotUsed] =
    Flow[DependencyCount].fold(new AccumulatedDependencyCount)({ (acc, dep) =>
      acc.addDep(dep)
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
      .buffer(10, OverflowStrategy.backpressure)
      .via(ObjectsThrottle)
      .via(flowFetchDependencies)
      .via(flowMapVersions)
      .buffer(12, OverflowStrategy.backpressure)
      .via(flowDependencies)
      .via(flowAccumulateDependencyCounts)
      .to(sink)
  runnableGraph.run()
}
