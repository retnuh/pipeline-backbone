package ie.zalando.pipeline.backbone.concurrent

import java.util.concurrent.{ Callable, Future, ExecutorService }

import scala.util.Try

import org.slf4j.LoggerFactory

import cats.data.Xor
import ie.zalando.pipeline.backbone.Backbone
import ie.zalando.pipeline.backbone.Phases.{ LocalReleasePhase, TransformationPipelineFailure }

class ExecutorServiceBackboneCoordinator[DA](backbone: Backbone[DA], executor: ExecutorService) {
  import ExecutorServiceBackboneCoordinator._

  val localInitPhases = backbone.initializeTopLevelContexts

  private class BackboneCallable(datum: DA) extends Callable[Xor[TransformationPipelineFailure, DA]] {
    override def call(): Xor[TransformationPipelineFailure, DA] = {
      val (ts, releasers) = backbone.initializeInLocalContext(-1, localInitPhases).unzip
      try {
        backbone.transformDatum(backbone.createStateMonad(ts), datum)
      } finally {
        releasers.foreach((phase: LocalReleasePhase) => {
          Try({ phase.releaseLocalResources() }).recover { case ex => log.warn(s"Release phase $phase failed:", ex) }
        })
      }
    }
  }

  def process(datum: DA): Future[Xor[TransformationPipelineFailure, DA]] = {
    executor.submit(new BackboneCallable(datum))
  }

}

object ExecutorServiceBackboneCoordinator {
  val log = LoggerFactory.getLogger(classOf[ExecutorServiceBackboneCoordinator[_]])
}
