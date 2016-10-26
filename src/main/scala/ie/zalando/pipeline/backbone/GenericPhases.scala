package ie.zalando.pipeline.backbone

import cats.data.Xor

object GenericPhases {

  import ie.zalando.pipeline.backbone.Phases._

  def pred[DA](predicate: DA => Boolean, reason: String): GenericPredicateTopLevelInitPhase[DA] =
    GenericPredicateTopLevelInitPhase(predicate, reason)

  def xform[DA](xform: DA => DA): GenericTransformationTopLevelInitPhase[DA] =
    GenericTransformationTopLevelInitPhase(xform)

  case class GenericPredicateDatumPhase[DA](predicate: DA => Boolean, reason: String) extends DatumPhase[DA] {
    override def transformDatum(datum: DA): Xor[TransformationPipelineFailure, DA] = {
      if (predicate(datum)) {
        Xor.right(datum)
      } else {
        Xor.left(TransformationPipelineStopped(reason))
      }
    }
  }

  case class GenericPredicateLocalInitPhase[DA](predicate: DA => Boolean, reason: String) extends LocalInitializationPhase[DA] {
    override type DP = GenericPredicateDatumPhase[DA]
    override type LRP = NoOpReleasePhase.type

    override def initializeInLocalContext(partition: Int): (DP, LRP) = {
      (GenericPredicateDatumPhase(predicate, reason), NoOpReleasePhase)
    }

  }

  case class GenericPredicateTopLevelInitPhase[DA](predicate: DA => Boolean, reason: String) extends TopLevelInitializationPhase[DA] {
    override type LIP = GenericPredicateLocalInitPhase[DA]

    override def initializeInTopLevelContext: LIP = {
      GenericPredicateLocalInitPhase(predicate, reason)
    }
  }

  case class GenericTransformationDatumPhase[DA](transformation: DA => DA) extends DatumPhase[DA] {
    override def transformDatum(datum: DA): Xor[TransformationPipelineFailure, DA] = {
      Xor.right(transformation(datum))
    }
  }

  case class GenericTransformationLocalInitPhase[DA](transformation: DA => DA) extends LocalInitializationPhase[DA] {
    override type DP = GenericTransformationDatumPhase[DA]
    override type LRP = NoOpReleasePhase.type

    override def initializeInLocalContext(partition: Int): (DP, LRP) = {
      (GenericTransformationDatumPhase(transformation), NoOpReleasePhase)
    }

  }

  case class GenericTransformationTopLevelInitPhase[DA](transformation: DA => DA) extends TopLevelInitializationPhase[DA] {
    override type LIP = GenericTransformationLocalInitPhase[DA]

    override def initializeInTopLevelContext: LIP = {
      GenericTransformationLocalInitPhase(transformation)
    }
  }
}
