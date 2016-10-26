package ie.zalando.pipeline.backbone.concurrent

import scala.concurrent.ExecutionContext

import org.scalatest.concurrent.ScalaFutures
import org.scalatest.{ FlatSpec, Matchers }

import ie.zalando.pipeline.backbone.PhaseTrackingPhase.PhaseTrackingTopLevelInitPhase
import ie.zalando.pipeline.backbone.Phases.TopLevelInitializationPhase
import ie.zalando.pipeline.backbone.TestDatum

class ExecutionContextBackboneCoordinatorSpec extends FlatSpec with Matchers with ScalaFutures {

  "An ExecutionContextBackboneCoordinator" should "push a datum through the pipeline" in new Fixture {
    val ec = ExecutionContext.global
    val coordinator = new ExecutionContextBackboneCoordinator(backbone, ec)
    val f1 = coordinator.process(TestDatum(name = "Megatron"))
    val f2 = coordinator.process(TestDatum(name = "Soundwave"))
    val f3 = coordinator.process(TestDatum(name = "Shockwave"))

    f1.futureValue.isRight shouldBe true
    f1.futureValue.foreach(_.phrase shouldBe "Hello, Megatron, this was calculated on partition -1")
    f1.futureValue.foreach(_.wordCount shouldBe 8)
    f1.futureValue.foreach(_.isEven shouldBe Some(true))

    f2.futureValue.isRight shouldBe true
    f2.futureValue.foreach(_.phrase shouldBe "Hello, Soundwave, this was calculated on partition -1")
    f2.futureValue.foreach(_.wordCount shouldBe 8)
    f2.futureValue.foreach(_.isEven shouldBe Some(false))

    f3.futureValue.isRight shouldBe true
    f3.futureValue.foreach(_.phrase shouldBe "Hello, Shockwave, this was calculated on partition -1")
    f3.futureValue.foreach(_.wordCount shouldBe 8)
    f3.futureValue.foreach(_.isEven shouldBe Some(false))
  }

  "An ExecutionContextBackboneCoordinator" should "intialize and release the local phases properly" in new Fixture {

    override def driverInitPhases: Seq[TopLevelInitializationPhase[TestDatum]] = super.driverInitPhases :+ PhaseTrackingTopLevelInitPhase()

    val ec = ExecutionContext.global
    val coordinator = new ExecutionContextBackboneCoordinator(backbone, ec)
    val f1 = coordinator.process(TestDatum(name = "Megatron"))
    val f2 = coordinator.process(TestDatum(name = "Soundwave"))
    val f3 = coordinator.process(TestDatum(name = "Shockwave"))

    f1.futureValue.isRight shouldBe true
    f2.futureValue.isRight shouldBe true
    f3.futureValue.isRight shouldBe true
    f1.futureValue.foreach(_.phrase shouldBe "Hello, Megatron, this was calculated on partition -1")
    f2.futureValue.foreach(_.phrase shouldBe "Hello, Soundwave, this was calculated on partition -1")
    f3.futureValue.foreach(_.phrase shouldBe "Hello, Shockwave, this was calculated on partition -1")
    f1.futureValue.foreach(_.localReleased.get.get shouldBe true)
    f2.futureValue.foreach(_.localReleased.get.get shouldBe true)
    f3.futureValue.foreach(_.localReleased.get.get shouldBe true)
  }
}
