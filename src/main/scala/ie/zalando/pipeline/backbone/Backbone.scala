package ie.zalando.pipeline.backbone

import scala.util.Try

import org.slf4j.LoggerFactory

import cats.data.{ State, Xor }
import ie.zalando.pipeline.backbone.Phases.{ DatumPhase, LocalInitializationPhase, LocalReleasePhase, TopLevelInitializationPhase, TransformationPipelineError, TransformationPipelineFailure }

/**
 * The Backbone class is meant to coordinate the transition and running of various Phases in a data pipeline.
 * It does not actually have any methods that tie these transitions together at a high level, that is left to a per
 * framework/library class such as KafkaStreamsBackboneCoordinator.
 *
 * @param topLevelPhases  The top-level phases that this Backbone will coordinate the phase transitions of.
 * @tparam DA The type of the object that the data pipeline actually manipulates.
 */
case class Backbone[DA](topLevelPhases: Seq[TopLevelInitializationPhase[DA]]) {

  /**
   * A convenience type to to represent a failure part way through the data pipeline.
   */
  type FailureOrData = Xor[TransformationPipelineFailure, DA]

  /**
   * The type used by the State Monad during the data transformation stage.
   */
  // TODO - when there is an error, we might want the state to return a bool as to
  // whether or not to continue?
  type DatumTransformationState = State[FailureOrData, Unit]

  val log = LoggerFactory.getLogger(getClass)

  /**
   * To be called when we actually want to initialize the top level contexts.
   *
   * @return a sequence of LocalInitializationPhases.
   */
  def initializeTopLevelContexts: Seq[LocalInitializationPhase[DA]] =
    topLevelPhases.map(phase => phase.initializeInTopLevelContext)

  /**
   * To be called when we want to initialize "local" contexts, this would correspond to thread-local variables, etc.
   *
   * @param partition Experimental.  Could correspond the Spark partition number, or the partition of a Kafka topic.
   *                  May be removed in the future.
   * @param localInitializationPhases The localInitializationPhases that will be run.
   * @return a sequence of tuples of (DatumPhase, LocalReleasePhase) returned by each LocalInitializationPhases
   *         initializeInLocalContext method.
   */
  def initializeInLocalContext(partition: Int, localInitializationPhases: Seq[LocalInitializationPhase[DA]]): Seq[(LocalInitializationPhase[DA]#DP, LocalInitializationPhase[DA]#LRP)] =
    localInitializationPhases.map(phase => phase.initializeInLocalContext(partition))

  /**
   * To be called when we want to release locally allocated state, such as when closing a KafakaStream application or
   * finishing mapping over all the data in a given partition in Spark.
   *
   * @param phases The LocalReleasePhases to call.
   */
  def releasePartitionResources(phases: Seq[LocalReleasePhase]): Unit = {
    phases.foreach(phase => {
      log.debug("Releasing phase {}", phase)
      Try({
        phase.releaseLocalResources()
      }).recover({ case ex: Exception => log.warn(s"Error while releasing resources for phase $phase, ignoring:", ex) })
    })
  }

  /**
   * Compute the state monad that represents the chaining of the datum phases together, along with appropriate error
   * handling and propagation.
   */
  def createStateMonad(phases: Seq[DatumPhase[DA]]): DatumTransformationState = {
    val foldStart: DatumTransformationState = State.modify(identity)
    phases.foldLeft(foldStart)((state: DatumTransformationState, phase: DatumPhase[DA]) => {
      state.flatMap(Unit => State.modify((xor: FailureOrData) => xor.flatMap(da => {
        Try({
          phase.transformDatum(da)
        }).recover({
          case ex: Exception =>
            log.info(s"Something went wrong in phase ${phase}: ", ex)
            Xor.left[TransformationPipelineFailure, DA](TransformationPipelineError(ex))
        }).get
      })))
    })
  }

  /**
   * Once resources have been allocated at a top-level and also at a local level, we actually want to create and run
   * a transformation pipeline using the given DatumPhases.  We use a StateMonad to create an object that represents
   * the chaining of the datum phases together, along with appropriate error handling and propagation.  Once the state
   * monad is created, we run all the actual data through it.
   *
   * @param phases The DatumPhases to be used to create the transformation pipeline.
   * @param iterator An iterator over the actual data.
   * @return a sequence of FailureOrData representing the results of each datums transformation.
   */
  def transformData(phases: Seq[DatumPhase[DA]], iterator: Iterator[DA]): Seq[FailureOrData] = {
    val xformStateMonad = createStateMonad(phases)
    iterator.map((da: DA) => xformStateMonad.runS(Xor.right(da)).value).toSeq
  }

  /**
   * Transform a single datum.
   *
   * @param xformStateMonad  The StateMonad created with a call to createStateMonad
   * @param datum The Datum
   * @return The FailureOrData representing the result of the transformation.
   */
  def transformDatum(xformStateMonad: DatumTransformationState, datum: DA): FailureOrData = {
    xformStateMonad.runS(Xor.right(datum)).value
  }
}
