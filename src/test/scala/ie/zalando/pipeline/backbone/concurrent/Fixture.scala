package ie.zalando.pipeline.backbone.concurrent

import ie.zalando.pipeline.backbone.CountWordsPhase.CountWordsTopLevelInitPhase
import ie.zalando.pipeline.backbone.IsEvenPhase.IsEvenTopLevelInitPhase
import ie.zalando.pipeline.backbone.Phases.TopLevelInitializationPhase
import ie.zalando.pipeline.backbone.SayHelloPhase.SayHelloTopLevelInitPhase
import ie.zalando.pipeline.backbone.{ Backbone, TestDatum }

trait Fixture {
  def driverInitPhases: Seq[TopLevelInitializationPhase[TestDatum]] = List(SayHelloTopLevelInitPhase(), CountWordsTopLevelInitPhase(), IsEvenTopLevelInitPhase())
  val backbone = Backbone[TestDatum](driverInitPhases)
}
