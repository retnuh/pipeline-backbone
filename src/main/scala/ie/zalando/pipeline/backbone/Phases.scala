package ie.zalando.pipeline.backbone

import scala.concurrent.duration.Duration

import cats.data.Xor

/**
 * To implement a transformation stage in a backbone, a series of phases need to be implemented to represent
 * the initialization lifecycle that may be needed along the way.  All data is represented using immutable case classes
 * to avoid unknown/unexpected threading interactions.  Each phases also declares the type(s) of the phase(s) that it
 * will return.  This is mainly to help the compiler not get upset.
 *
 * The TopLevelInitializationPhase is used to load any "once-off" data, such as a Spark classifier model, read
 * configuration from a resource or external file, etc.  Any data loaded/created at this point must have operations
 * that are thread safe.
 *
 * The LocalInitializationPhase is used to allocate "local" resources, such as non-thread-safe objects, borrow something
 * from an object pool, etc.
 *
 * The LocalReleasePhase is used to clean up an local resources - clear ThreadLocal variables, return objects to pools, etc.
 *
 * The DatumPhase is the place where actual data transformation takes place.
 */
case object Phases {

  /**
   * A top-level trait used to capture different types of stoppages within a given transformation pipeline.
   */
  trait TransformationPipelineFailure

  /**
   * This should be used to capture that there was some kind of unexpected error while trying to process a particular datum.
   * @param ex The (presumeably unexpected) exception thrown during processing.
   */
  case class TransformationPipelineError(ex: Throwable) extends TransformationPipelineFailure

  /**
   * This should be used when we want to stop further processing of a particular datum, but not due to error.  For example,
   * a classifier DatumPhase may decide the the data shouldn't be processed further.
   * @param reason A human understandable reason why this data wasn't processed further.
   */
  case class TransformationPipelineStopped(reason: String) extends TransformationPipelineFailure

  /**
   * This should be used when we want to indicate that it took too long for the pipeline to process a particular datum.
   * @param duration The amount of time that the datum failed to process within.
   */
  case class TransformationPipelineTimeout(duration: Duration) extends TransformationPipelineFailure

  /**
   * A Phase representing the loading of top-level, thread safe data.
   * @tparam DA the type of the data that will be manipulated in the DatumPhase.
   */
  trait TopLevelInitializationPhase[DA] extends Serializable {
    /**
     * The type of the LocalInitializationPhase that this phase will create.
     */
    type LIP <: LocalInitializationPhase[DA]

    /**
     * Do the actual top-level initialization, if any.  Any data returned at this point is assumed to be thread-safe.
     * @return The LococalInitializationPhase containing all necessary information/objects/data/whatever calculated at
     *         this point in the lifecycle.
     */
    def initializeInTopLevelContext: LIP
  }

  /**
   * A Phase representing the loading of any "local" data.  "Local" here can be assumed to be thread local, so any
   * objects that are not thread safe can be created, borrowed from a pool, etc.
   * @tparam DA
   */
  trait LocalInitializationPhase[DA] extends Serializable {
    /**
     * The type of the DatumPhase that will be returned.
     */
    type DP <: DatumPhase[DA]
    /**
     * The type of the LocalReleasePhase that will be returned.
     */
    type LRP <: LocalReleasePhase

    /**
     * Do the actual "local" initialization.
     * @param partition Experimental.  Could be the partition number of data running on a given Spark partition,
     *                  or the partition number of a kafka topic that data is being read from.  May be removed in the
     *                  future, it's main use is probably in debugging, to where data is coming from.
     * @return A Tuple of (DatumPhase, LocalReleasePhase).  Both objects need to have access to any data they need to
     *         perform their respective jobs.
     */
    def initializeInLocalContext(partition: Int): (DP, LRP)
  }

  /**
   * The phase where actual data transformation happens.  Finally.  Hooray!
   */
  trait DatumPhase[DA] extends Serializable {
    /**
     * Do the actual data transformation on a given datum.
     * @param datum the datum to be transformed
     * @return an Xor[TransformationPipelineFailure, DA] representing either a failure/stoppage (left), or the transformed
     *         datum (right).
     */
    def transformDatum(datum: DA): Xor[TransformationPipelineFailure, DA]
  }

  /**
   * A Phase to allow cleanup/releasing any resources created/allocated/borrowed/whatever by the LocalInitializationPhase.
   */
  trait LocalReleasePhase extends Serializable {
    /**
     * Do the actual release of any open resources, return objects to pools, etc.
     */
    def releaseLocalResources(): Unit
  }

  /**
   * A handy singleton that can be used for a group of phases that has no local clean up.
   */
  case object NoOpReleasePhase extends LocalReleasePhase {
    override def releaseLocalResources(): Unit = {}
  }

}
