//package scenario3

import akka.NotUsed
import akka.actor.typed.ActorSystem
import akka.actor.typed.scaladsl.Behaviors
import akka.stream.Materializer
import akka.stream.scaladsl.{Sink, Source}
import scenario3.{KafkaProducerService, MetricGenerator}

import scala.concurrent.duration._

object Main {
  def main(args: Array[String]): Unit = {
    val system = ActorSystem[Nothing](Behaviors.empty, "ServerMetricsSystem")
    implicit val ec = system.executionContext
    implicit val materializer: Materializer = Materializer(system)

    val kafkaProducerService = new KafkaProducerService("localhost:9092", "server-metrics")

    Source.tick(0.seconds, 5.seconds, NotUsed)
      .map(_ => MetricGenerator.generateMetric())
      .runWith(Sink.foreach(metric => kafkaProducerService.send(metric)))

    system.log.info("Server Metrics Microservice started")
  }
}

