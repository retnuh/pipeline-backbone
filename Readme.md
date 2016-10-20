# Zalando Fashion Insight Centre Pipeline Backbone

## Motivation

There are a lot of different frameworks that offer different ways of assembling data transformation pipelines.  Indeed, Spark makes this fairly trivial with calls to `map` and `filter`, or by using `DataFrames`/`DataSets`.  However, the complexity can often increase quite dramatically the moment you step outside the trivial and into a world where you need more control.  Want consistent error handling?  Write it yourself and add it to every call.  Need to keep track of values filtered early in the pipeline to be output on an error topic during final processing?  Roll your own state monad and tweak every call as you go along.  Want custom metrics on stage timings?  Roll your own...  you get the idea.

Once you step outside the provided box, things can get pretty ugly, especially when you also need to consider things like thread safety, dealing with expensive-to-create but thread unsafe objects like lexicons, third-party libraries, etc, etc.

The goal of the backbone is to create a consistent way of being able to create data transformation pipelines that could potentially work on different frameworks, without having to re-write your code every time.  It was initially written with a target backend of Spark in mind, but during development the decision was made that the first deployment to use the Backbone would actually be on Kafka Streams.  Because of the way it had been written, this was a straightforward change with no real loss of development time - the planned Spark integration was shelved and the appropriate Kafka Streams integration written instead, with only trivial changes to the backbone required to support the Kafka model.

## Design decisions

It was decided early on that a functional style would be used as much as possible;  the Backbone and the various Phases (discussed below) would be implemented using immutable case classes; the Backbone takes sequences of phases and asks each of them for their next logical phase.  All state is captured in the Phase case classes, and all methods on the backbone are purely functional.

While it may seem initially a little cumbersome to have to implement all the phases for each transformation, there are a few benefits.  It explicitly seperates out global initialization where one-off configuration and loading of thread safe objects can take place, from "local" initialization, where maybe objects from a third-party library that are known to be not thread-safe can be initialized properly.  Care is also taken around the proper release of objects to avoid memory/resource leaks.

## Design

The main concept centers around the idea of a _Phase_.  Unfortunately Spark terminology has very clear meanings already for _Job_, _Stage_, and _Task_, and we didn't want to clash or confuse.  The Phases sort of represent some sort of Lifecycle stage, but not exactly, so _Phase_ was settled on as being reasonable and not getting in the way.

Each individual transformation is represented by 4 phases.

### `TopLevelInitializationPhase`

The `TopLevelInitializationPhase` is used to handle any loading of configuration and creating any thread-safe objects that will be used during either the `LocalInitializationPhase` or the `DatumPhase`.  Anything needed is passed to the constructor of the case class that represents the next _Phase_, the `LocalInitializationPhase`.  Any objects created here are assumed to be okay to live for the lifetime of the process and no effort is made to release them.

The initialization actually happens when the `initializeInTopLevelContext` method is called by the Backbone.

An example from the `ContentClassifierTopLevelInitPhase` follows, where it loads some data from S3 and loads a Spark SVM model:

```{scala}
    override def initializeInTopLevelContext: ContentClassifierLocalInitializationPhase = {

      val config = ConfigFactory.load("backbone.conf")
      val modelPath = config.getString("data-input.model-path")

      implicit val sparkContext: SparkContext = createContext(config)
      val svmModel: SVMModel = ClassifierIO.readModel(modelPath)
      val (maxLenUrl, maxLenLast) = ClassifierIO.readMaxsS3(sparkContext, config.getString("data-input.max-params-path"))
      sparkContext.stop()
      ContentClassifierLocalInitializationPhase(svmModel, maxLenUrl, maxLenLast)

    }
```

### `LocalInitializationPhase`

The `LocalInitializationPhase` is used to handle loading of objects that need to be used on a per-thread basis.  Anything needed by the `DatumPhase` or the `LocalReleasePhase` needs to be passed to the respective case class constructors for those two objects.  

Objects that need special release or handling one the "local context" goes away can be cleaned up by the `LocalReleasePhase`.  Examples
might be resetting `ThreadLocal` variables, returning objects to object pools, etc.

The initialization actually happens when the `initializeInLocalContext` method is called by the backbone.

An example from the `DateExtractionLocalInitializationPhase` demonstrates.  Note that it's objects don't need special release handling, so it uses the included `NoOpReleasePhase` case object.

```{scala}
    override def initializeInLocalContext(partition: Int): (DateExtractionDatumPhase, NoOpReleasePhase.type) =
      (new DateExtractionDatumPhase(DefaultDateExtractor.createExtractor), NoOpReleasePhase)
```

### `LocalReleasePhase`

The `LocalReleasePhase` releases any objects that need special release handling, such as calling a `close()` method, or returning an object to a pool, or whatever.

The motivation for this was the initial belief that some of the lexicon objects in the Entity detection portion of the pipeline would want to use an object pool, since the objects were expensive to create, and not thread safe.

Most transformations so far don't have any thing that needs explicit release management; they can use the provided `NoOpReleasePhase` case object.

The actual release should happen when the `releaseLocalResources` method is called by the backbone.

There are currently no examples that explicitly use a release phase; this may change when the Entity detection stuff has been completed.

### `DatumPhase`

The `DatumPhase` is the real meat of the transfomation; this is where all the transformation work actually happens.

The signature for the `transformDatum` method is:

```{scala}
    def transformDatum(datum: DA): Xor[TransformationPipelineFailure, DA]
```

`DA` is a parametric type representing the actual case class that is being used to carry all the fields as the object gets moved along the transformation pipeline.

The return value is an `Xor`, which is quite like an `Either`, but is in some ways friendlier to use.  It is used to differentiate between a transformation that has completed successfully and should carry on, in which case an updated copy of the datum is returned, or an instance of a subclass of `TransformationPipelineFailure` is returned.  The backbone itself handles catching of exceptions thrown during transformation; in this case a `TransformationPipelineError` is returned, along with the exception that was thrown.  However, it is perfectly acceptable for the transformation to decide that further transformations shouldn't be performed; it can do this by returning an instance of `TransformationPipelineStopped`.

The following example from the `ContentClassifierDatumPhase` demonstrates both a normal trasformation, and and example of stopping further transformations:

```{scala}
    override def transformDatum(datum: TextPipelineDatum): Xor[TransformationPipelineFailure, TextPipelineDatum] = {

      val published = if (datum.publishedDate != null) true else false
      val isContent = PredictPageClassifier.featuresPredict(datum.url.toString, published, maxLenUrl, maxLenLast, svmModel)

      if (isContent) {
        Xor.right(datum.copy(isContent = true))
      } else
        Xor.left(TransformationPipelineStopped(s"${datum.url} was classified as non-content"))
    }
```

Internally, the backbone uses a `StateMonad` as a way of chaining together all the DatumPhases, along with appropriate error handling.  Without going into it too deeply, the StateMonad is a nice way of creating an object that represents further computations to be done, given an input.  It is a purely functional construct that fits in well with the guiding philosophies used in the Backbone.

## Backbone Coordinators

As was mentioned, initially the plan was to release on Spark, but the decision was made to deploy on Kafka Streams instead.  Currently there is a class called `KafkaStreamsBackboneCoordinator` that represents the essential glue between the Backbone and Kafka Streams.

There is also a an `ExecutorServiceBackboneCoordinator` that allows running a Backbone Pipeline in-process using a passed in `ExecutorService` to handle concurrency.

It would still be possible to write a `SparkStreamingBackboneCoordinator` or something similar, but there are currently no plans.

Given that each framework/library integration will be very specific to the particular framework/library, there is no top level interface/trait/abstract class defined for the coordinators.  A marker interface/trait could be added, but there is currently no need/gain seen for this.
