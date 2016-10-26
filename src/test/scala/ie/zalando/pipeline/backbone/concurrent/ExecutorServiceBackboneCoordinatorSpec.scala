package ie.zalando.pipeline.backbone.concurrent

import java.util.concurrent.Executors

import org.scalatest.{ FlatSpec, Matchers }

import ie.zalando.pipeline.backbone.PhaseTrackingPhase.PhaseTrackingTopLevelInitPhase
import ie.zalando.pipeline.backbone.Phases.TopLevelInitializationPhase
import ie.zalando.pipeline.backbone.TestDatum

class ExecutorServiceBackboneCoordinatorSpec extends FlatSpec with Matchers {

  "An ExecutorServiceBackboneCoordinator" should "push a datum through the pipeline" in new Fixture {
    val executor = Executors.newSingleThreadExecutor()
    try {
      val coordinator = new ExecutorServiceBackboneCoordinator(backbone, executor)
      val f1 = coordinator.process(TestDatum(name = "Megatron"))
      val f2 = coordinator.process(TestDatum(name = "Soundwave"))
      val f3 = coordinator.process(TestDatum(name = "Shockwave"))

      f1.get.isRight shouldBe true
      f1.get.foreach(_.phrase shouldBe "Hello, Megatron, this was calculated on partition -1")
      f1.get.foreach(_.wordCount shouldBe 8)
      f1.get.foreach(_.isEven shouldBe Some(true))
      f2.get.isRight shouldBe true
      f2.get.foreach(_.phrase shouldBe "Hello, Soundwave, this was calculated on partition -1")
      f2.get.foreach(_.wordCount shouldBe 8)
      f2.get.foreach(_.isEven shouldBe Some(false))
      f3.get.isRight shouldBe true
      f3.get.foreach(_.phrase shouldBe "Hello, Shockwave, this was calculated on partition -1")
      f3.get.foreach(_.wordCount shouldBe 8)
      f3.get.foreach(_.isEven shouldBe Some(false))
    } finally {
      executor.shutdown()
    }
  }

  "An ExecutorServiceBackboneCoordinator" should "intialize and release the local phases properly" in new Fixture {

    override def driverInitPhases: Seq[TopLevelInitializationPhase[TestDatum]] = super.driverInitPhases :+ PhaseTrackingTopLevelInitPhase()

    val executor = Executors.newWorkStealingPool(3)
    try {
      val coordinator = new ExecutorServiceBackboneCoordinator(backbone, executor)
      val f1 = coordinator.process(TestDatum(name = "Megatron"))
      val f2 = coordinator.process(TestDatum(name = "Soundwave"))
      val f3 = coordinator.process(TestDatum(name = "Shockwave"))

      f1.get.isRight shouldBe true
      f2.get.isRight shouldBe true
      f3.get.isRight shouldBe true
      f1.get.foreach(_.phrase shouldBe "Hello, Megatron, this was calculated on partition -1")
      f2.get.foreach(_.phrase shouldBe "Hello, Soundwave, this was calculated on partition -1")
      f3.get.foreach(_.phrase shouldBe "Hello, Shockwave, this was calculated on partition -1")
      f1.get.foreach(_.localReleased.get.get shouldBe true)
      f2.get.foreach(_.localReleased.get.get shouldBe true)
      f3.get.foreach(_.localReleased.get.get shouldBe true)
    } finally {
      executor.shutdown()
    }
  }
}
