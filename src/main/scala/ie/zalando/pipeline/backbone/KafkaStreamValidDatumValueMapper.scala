package ie.zalando.pipeline.backbone

import java.lang.{ Iterable => JIterable }

import scala.collection.JavaConverters._

import org.apache.kafka.streams.kstream.ValueMapper

import cats.data.Xor
import ie.zalando.pipeline.backbone.Phases.TransformationPipelineFailure

/**
 * A simple class to act as a filter on FailureOrData (represented by Xor[TransformationPipelineFailure, DA]), returning
 * only the objects that had valid transformations.
 *
 * @tparam DA The datum type to be filtered over.
 */
class KafkaStreamValidDatumValueMapper[DA] extends ValueMapper[Xor[TransformationPipelineFailure, DA], JIterable[DA]] {
  override def apply(failureOrValue: Xor[TransformationPipelineFailure, DA]): JIterable[DA] = failureOrValue.toList.asJava
}
