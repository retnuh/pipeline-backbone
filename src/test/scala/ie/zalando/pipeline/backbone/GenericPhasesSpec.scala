package ie.zalando.pipeline.backbone

import scala.concurrent.ExecutionContext

import org.scalatest.concurrent.ScalaFutures
import org.scalatest.{ FlatSpec, Matchers }

import cats.data.Xor
import ie.zalando.pipeline.backbone.Phases.TransformationPipelineStopped
import ie.zalando.pipeline.backbone.concurrent.ExecutionContextBackboneCoordinator

class GenericPhasesSpec extends FlatSpec with Matchers with ScalaFutures {

  import GenericPhases._

  case class IntHolder(x: Int)

  def add10(xh: IntHolder): IntHolder = xh.copy(x = xh.x + 10)

  def isEven(xh: IntHolder): Boolean = xh.x % 2 == 0

  "The generic phases" should "allow use of a backbone using simple functions with minimal boilerplate" in {
    val phases = Seq(xform(add10), pred(isEven, "odd"), xform(add10))
    val backbone = Backbone(phases)
    val ec = ExecutionContext.global
    val coordinator = new ExecutionContextBackboneCoordinator(backbone, ec)

    val f1 = coordinator.process(IntHolder(1))
    val f2 = coordinator.process(IntHolder(2))

    f1.futureValue should matchPattern { case Xor.Left(TransformationPipelineStopped("odd")) => }
    f2.futureValue should matchPattern { case Xor.Right(IntHolder(22)) => }
  }
}
