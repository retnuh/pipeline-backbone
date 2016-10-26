package ie.zalando.pipeline.backbone.concurrent

import scala.concurrent.{ ExecutionContext, Future }
import scala.util.Try

import org.slf4j.LoggerFactory

import cats.data.Xor
import ie.zalando.pipeline.backbone.Backbone
import ie.zalando.pipeline.backbone.Phases.{ LocalReleasePhase, TransformationPipelineFailure }

class ExecutionContextBackboneCoordinator[DA](backbone: Backbone[DA], executionContext: ExecutionContext) {

  import ExecutionContextBackboneCoordinator._

  val localInitPhases = backbone.initializeTopLevelContexts

  def process(datum: DA): Future[Xor[TransformationPipelineFailure, DA]] = {
    Future {
      val (dataPhases, releasePhases) = backbone.initializeInLocalContext(-1, localInitPhases).unzip
      try {
        backbone.transformDatum(backbone.createStateMonad(dataPhases), datum)
      } finally {
        releasePhases.foreach((phase: LocalReleasePhase) => {
          Try({ phase.releaseLocalResources() }).recover { case ex => log.warn(s"Release phase $phase failed:", ex) }
        })
      }
    }(executionContext)
  }

}

object ExecutionContextBackboneCoordinator {
  val log = LoggerFactory.getLogger(classOf[ExecutorServiceBackboneCoordinator[_]])
}
