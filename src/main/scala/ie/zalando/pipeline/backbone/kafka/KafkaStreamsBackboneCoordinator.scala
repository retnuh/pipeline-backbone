package ie.zalando.pipeline.backbone.kafka

import java.util.concurrent.TimeoutException

import scala.concurrent.duration.Duration
import scala.concurrent.{ Await, Future }
import scala.util.Try

import org.apache.kafka.streams.kstream.{ ValueTransformer, ValueTransformerSupplier }
import org.apache.kafka.streams.processor.ProcessorContext
import org.slf4j.LoggerFactory

import cats.data.Xor
import ie.zalando.pipeline.backbone.Backbone
import ie.zalando.pipeline.backbone.Phases._

/**
 * The KafkaStreamsBackboneCoordinator is the framework-specific implementation of a Backbone using KafkaStreams.
 * Most of the code here is very specific to the Kafka Streams library/framework.
 *
 * At a high level, the KafkaStreamsBackboneCoordinator can be plugged into a KafkaStream, and it will create instances
 * of KafkaStreamsBackboneValueTransformer that will run the Backbone transformData stage on each piece of data going
 * through the KafkaStream.
 *
 * Currently the running of the various backbone phases is split across the KafkaStreamsSBackboneCoordinator and the
 * KafkaStreamsBackboneValueTransformer, as appropriate.
 *
 * @param backbone The backbone to be used
 * @tparam DA The type of the data that will be transformed
 */
case class KafkaStreamsBackboneCoordinator[DA](backbone: Backbone[DA], duration: Duration = Duration.Inf)
    extends ValueTransformerSupplier[DA, Xor[TransformationPipelineFailure, DA]] {
  import KafkaStreamsBackboneCoordinator._

  val localInitPhases = backbone.initializeTopLevelContexts
  override def get(): ValueTransformer[DA, Xor[TransformationPipelineFailure, DA]] = {
    log.info("get() called")
    KafkaStreamsBackboneValueTransformer(backbone, localInitPhases, duration)
  }
}

private object KafkaStreamsBackboneCoordinator {
  val log = LoggerFactory.getLogger(classOf[KafkaStreamsBackboneCoordinator[_]])
}

private case class KafkaStreamsBackboneValueTransformer[DA](backbone: Backbone[DA], localInitPhases: Seq[LocalInitializationPhase[DA]], duration: Duration)
    extends ValueTransformer[DA, Xor[TransformationPipelineFailure, DA]] {
  import scala.concurrent.ExecutionContext.Implicits._

  import KafkaStreamsBackboneValueTransformer._

  private var xformStateMonadOption: Option[backbone.DatumTransformationState] = None
  private var releasers: Option[Seq[LocalReleasePhase]] = None

  override def init(context: ProcessorContext): Unit = {
    log.info(s"Init called with ${context.toString} ${context.taskId}")
    val (dataPhases, releasePhases) = backbone.initializeInLocalContext(context.taskId.partition, localInitPhases).unzip
    xformStateMonadOption = Option(backbone.createStateMonad(dataPhases))
    releasers = Option(releasePhases)
  }

  override def punctuate(timestamp: Long): Xor[TransformationPipelineFailure, DA] = {
    log.debug(s"punctuate called at $timestamp")
    null
  }

  override def close(): Unit = {
    xformStateMonadOption = None
    releasers.foreach((phases: Seq[LocalReleasePhase]) => phases.foreach((phase: LocalReleasePhase) => phase.releaseLocalResources()))
    releasers = None
  }

  override def transform(value: DA): Xor[TransformationPipelineFailure, DA] = {
    xformStateMonadOption.map((sm: backbone.DatumTransformationState) => {
      Try(Await.result(Future(backbone.transformDatum(sm, value)), duration)).recover {
        case _: TimeoutException =>
          log.warn(s"Timeout occured for datum: $value")
          Xor.Left(TransformationPipelineTimeout(value, duration))
      }.get
    }).get
  }
}

private object KafkaStreamsBackboneValueTransformer {
  val log = LoggerFactory.getLogger(classOf[KafkaStreamsBackboneValueTransformer[_]])
}
