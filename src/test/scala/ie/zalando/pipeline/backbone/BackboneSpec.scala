package ie.zalando.pipeline.backbone

import org.scalatest.EitherValues._
import org.scalatest.OptionValues._
import org.scalatest.{ FlatSpec, Matchers }

import cats.data.Xor
import ie.zalando.pipeline.backbone.CountWordsPhase._
import ie.zalando.pipeline.backbone.IsEvenPhase._
import Phases.LocalReleasePhase
import ie.zalando.pipeline.backbone.SayHelloPhase._
import Phases._
import ie.zalando.pipeline.backbone.CountWordsPhase.CountWordsTopLevelInitPhase
import ie.zalando.pipeline.backbone.IsEvenPhase.IsEvenTopLevelInitPhase
import ie.zalando.pipeline.backbone.SayHelloPhase.{ SayHelloReleasePhase, SayHelloTopLevelInitPhase }

class BackboneSpec extends FlatSpec with Matchers {

  val driverInitPhases = List(SayHelloTopLevelInitPhase(), CountWordsTopLevelInitPhase(), IsEvenTopLevelInitPhase())
  val backbone = Backbone[TestDatum](driverInitPhases)

  "A backbone's initialize in driver context" should
    "collect a seq of objects to be used in the PartitionInitializationPhase" in {
      val partitionInitPhases = backbone.initializeTopLevelContexts
      partitionInitPhases.length shouldBe 3
      partitionInitPhases should contain(driverInitPhases(0).initializeInTopLevelContext)
      partitionInitPhases should contain(driverInitPhases(1).initializeInTopLevelContext)
      partitionInitPhases should contain(driverInitPhases(2).initializeInTopLevelContext)
    }

  "A backbone's init in partition context" should
    "collect a seq of tuples of objects, each tuple containing a datum phase and a release dude" in {
      val tuples = backbone.initializeInLocalContext(10, backbone.initializeTopLevelContexts)
      tuples.length shouldBe 3
      tuples should contain(driverInitPhases(0).initializeInTopLevelContext.initializeInLocalContext(10))
      tuples should contain(driverInitPhases(1).initializeInTopLevelContext.initializeInLocalContext(10))
      tuples should contain(driverInitPhases(2).initializeInTopLevelContext.initializeInLocalContext(10))
    }

  "A backbone's release partition resources" should
    "call release on all the release objects even if one or more explodes" in {
      case object ExplodingReleasePhase extends LocalReleasePhase {
        override def releaseLocalResources(): Unit = throw new RuntimeException("Boom!")
      }
      val phases = List(SayHelloReleasePhase(0), SayHelloReleasePhase(2), SayHelloReleasePhase(4))
      backbone.releasePartitionResources(phases.zip(Stream.continually(ExplodingReleasePhase)).flatMap(x => List(x._1, x._2)))
      phases(0).released shouldBe Some(0)
      phases(1).released shouldBe Some(2)
      phases(2).released shouldBe Some(4)
    }

  "A backbones datum phase" should
    "call all the datum phases for each individual datum" in {
      val data = List(TestDatum(name = "Mr. Anderson"), TestDatum(name = "Agent Smith"), TestDatum(name = "Morpheus"))
      val tuples = backbone.initializeInLocalContext(10, backbone.initializeTopLevelContexts)
      val (datumPhases, _) = tuples.unzip
      val xformed = backbone.transformData(datumPhases, data.iterator)
      xformed(0).toOption.value shouldBe TestDatum("Mr. Anderson", "Hello, Mr. Anderson, this was calculated on partition 10", 9, Some(false))
      xformed(2).toOption.value shouldBe TestDatum("Morpheus", "Hello, Morpheus, this was calculated on partition 10", 8, Some(true))
    }

  "A backbones datum phase" should
    "call the datum phases for an individual datum" in {
      val datum = TestDatum(name = "Mr. Anderson")
      val tuples = backbone.initializeInLocalContext(10, backbone.initializeTopLevelContexts)
      val (datumPhases, _) = tuples.unzip
      val sm = backbone.createStateMonad(datumPhases)
      val xformed = backbone.transformDatum(sm, datum)
      xformed.toOption.value shouldBe TestDatum("Mr. Anderson", "Hello, Mr. Anderson, this was calculated on partition 10", 9, Some(false))
    }

  "A backbone datum phase" should
    "call the transformation pipeline even if an individual datum explodes" in {
      case class ExplodeOnDatumPhase(var i: Int) extends DatumPhase[TestDatum] {
        override def transformDatum(datum: TestDatum): Xor[TransformationPipelineFailure, TestDatum] = {
          i -= 1
          if (i == 0) {
            throw new RuntimeException("Data Pipeline Boom!")
          }
          Xor.right(datum)
        }
      }
      val exploder = ExplodeOnDatumPhase(2)
      val data = List(TestDatum(name = "Mr. Anderson"), TestDatum(name = "Agent Smith"), TestDatum(name = "Morpheus"))
      val tuples = backbone.initializeInLocalContext(10, backbone.initializeTopLevelContexts)
      val (datumPhases, _) = tuples.unzip
      val xformed = backbone.transformData(datumPhases.head :: exploder :: datumPhases.tail.toList, data.iterator)
      xformed(0).toOption.value shouldBe TestDatum("Mr. Anderson", "Hello, Mr. Anderson, this was calculated on partition 10", 9, Some(false))
      xformed(1).toEither.left.value should matchPattern { case TransformationPipelineError(ex: RuntimeException) => }
      xformed(2).toOption.value shouldBe TestDatum("Morpheus", "Hello, Morpheus, this was calculated on partition 10", 8, Some(true))
    }

  "An individual datum phase" should
    "be able to stop processing of a particular datum transformation" in {
      case object FilterBaddies extends DatumPhase[TestDatum] {
        override def transformDatum(datum: TestDatum): Xor[TransformationPipelineFailure, TestDatum] = {
          backbone.log.debug("Checking for agent: {} {}", datum.name, datum.name.contains("Agent"))
          if (datum.name.contains("Agent"))
            Xor.left(TransformationPipelineStopped("No Baddies!"))
          else
            Xor.right(datum)
        }
      }
      val data = List(TestDatum(name = "Mr. Anderson"), TestDatum(name = "Agent Smith"), TestDatum(name = "Morpheus"))
      val tuples = backbone.initializeInLocalContext(10, backbone.initializeTopLevelContexts)
      val (datumPhases, _) = tuples.unzip
      val xformed = backbone.transformData(datumPhases.head :: FilterBaddies :: datumPhases.tail.toList, data.iterator)
      xformed(0).toOption.value shouldBe TestDatum("Mr. Anderson", "Hello, Mr. Anderson, this was calculated on partition 10", 9, Some(false))
      backbone.log.debug(s"xformed(1): ${xformed(1)} ${xformed(1).toEither.leftSideValue}")
      xformed(1).toEither.left.value should matchPattern { case TransformationPipelineStopped(_) => }
      xformed(2).toOption.value shouldBe TestDatum("Morpheus", "Hello, Morpheus, this was calculated on partition 10", 8, Some(true))
    }

}
