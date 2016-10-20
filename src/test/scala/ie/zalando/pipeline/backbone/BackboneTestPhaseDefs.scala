package ie.zalando.pipeline.backbone

import java.util.concurrent.atomic.AtomicBoolean
import java.util.regex.Pattern

import cats.data.Xor
import ie.zalando.pipeline.backbone.Phases._

case class TestDatum(name: String, phrase: String = "", wordCount: Int = -1, isEven: Option[Boolean] = None,
  localReleased: Option[AtomicBoolean] = None)

// Phases for a simple "enrichment" transform

object SayHelloPhase {

  case class SayHelloDatumPhase(greeting: String, partition: Int) extends DatumPhase[TestDatum] {
    override def transformDatum(datum: TestDatum): Xor[TransformationPipelineFailure, TestDatum] = {
      Xor.right(datum.copy(phrase = s"$greeting, ${datum.name}, this was calculated on partition $partition"))
    }
  }

  case class SayHelloReleasePhase(partition: Int) extends LocalReleasePhase {
    var released: Option[Int] = None

    override def releaseLocalResources(): Unit = {
      released = Some(partition)
    }
  }

  case class SayHelloLocalInitPhase(greeting: String) extends LocalInitializationPhase[TestDatum] {
    type DP = SayHelloDatumPhase
    type LRP = SayHelloReleasePhase

    override def initializeInLocalContext(partition: Int): (SayHelloDatumPhase, SayHelloReleasePhase) = {
      (SayHelloDatumPhase(greeting, partition), SayHelloReleasePhase(partition))
    }
  }

  case class SayHelloTopLevelInitPhase() extends TopLevelInitializationPhase[TestDatum] {
    type LIP = SayHelloLocalInitPhase

    override def initializeInTopLevelContext: SayHelloLocalInitPhase = {
      // load greeting from somewhere or a config object passed in or whatevs
      SayHelloLocalInitPhase("Hello")
    }
  }

}

object CountWordsPhase {

  case class CountWordsDatumPhase(wordsPattern: Pattern) extends DatumPhase[TestDatum] {
    override def transformDatum(datum: TestDatum): Xor[TransformationPipelineFailure, TestDatum] = {
      Xor.right(datum.copy(wordCount = wordsPattern.split(datum.phrase).length))
    }
  }

  case class CountWordsLocalInitPhase(WordsPattern: Pattern) extends LocalInitializationPhase[TestDatum] {
    type LRP = NoOpReleasePhase.type
    type DP = CountWordsDatumPhase

    override def initializeInLocalContext(partition: Int): (CountWordsDatumPhase, NoOpReleasePhase.type) = {
      (CountWordsDatumPhase(WordsPattern), NoOpReleasePhase)
    }
  }

  val WordsPattern = Pattern.compile("[a-zA-Z]+")

  case class CountWordsTopLevelInitPhase() extends TopLevelInitializationPhase[TestDatum] {
    type LIP = CountWordsLocalInitPhase

    override def initializeInTopLevelContext: CountWordsLocalInitPhase = {
      CountWordsLocalInitPhase(WordsPattern)
    }
  }

}

object IsEvenPhase {

  case class IsEvenDatumPhase() extends DatumPhase[TestDatum] {
    def isEven(x: Int) = ((x & 0x01) == 0)

    override def transformDatum(datum: TestDatum): Xor[TransformationPipelineFailure, TestDatum] = {
      Xor.right(datum.copy(isEven = Some(isEven(datum.name.length) && isEven(datum.phrase.length) && isEven(datum.wordCount))))
    }
  }

  case class IsEvenLocalInitPhase() extends LocalInitializationPhase[TestDatum] {
    type DP = IsEvenDatumPhase
    type LRP = NoOpReleasePhase.type

    override def initializeInLocalContext(partition: Int): (IsEvenDatumPhase, NoOpReleasePhase.type) = {
      (IsEvenDatumPhase(), NoOpReleasePhase)
    }
  }

  case class IsEvenTopLevelInitPhase() extends TopLevelInitializationPhase[TestDatum] {
    type LIP = IsEvenLocalInitPhase

    override def initializeInTopLevelContext: IsEvenLocalInitPhase = {
      IsEvenLocalInitPhase()
    }
  }

}

object PhaseTrackingPhase {

  case class PhaseTrackingDatumPhase(released: AtomicBoolean) extends DatumPhase[TestDatum] {

    override def transformDatum(datum: TestDatum): Xor[TransformationPipelineFailure, TestDatum] = {
      Xor.right(datum.copy(localReleased = Option(released)))
    }
  }

  case class PhaseTrackingReleasePhase(released: AtomicBoolean) extends LocalReleasePhase {
    override def releaseLocalResources(): Unit = {
      released.set(true)
    }
  }

  case class PhaseTrackingInitializationPhase() extends LocalInitializationPhase[TestDatum] {
    override type DP = PhaseTrackingDatumPhase
    override type LRP = PhaseTrackingReleasePhase

    override def initializeInLocalContext(partition: Int): (DP, LRP) = {
      val released = new AtomicBoolean(false)
      (PhaseTrackingDatumPhase(released), PhaseTrackingReleasePhase(released))
    }
  }

  case class PhaseTrackingTopLevelInitPhase() extends TopLevelInitializationPhase[TestDatum] {
    override type LIP = PhaseTrackingInitializationPhase

    override def initializeInTopLevelContext: LIP = {
      PhaseTrackingInitializationPhase()
    }
  }
}
