package ie.zalando.pipeline.backbone.kafka

import java.util
import java.util.Properties

import scala.collection.mutable
import scala.concurrent.duration._

import org.apache.kafka.clients.producer.{ ProducerConfig, ProducerRecord }
import org.apache.kafka.common.serialization.{ Deserializer, Serde, Serdes, Serializer }
import org.apache.kafka.streams.kstream.{ ForeachAction, KStream, KStreamBuilder }
import org.apache.kafka.streams.processor.WallclockTimestampExtractor
import org.apache.kafka.streams.{ KafkaStreams, StreamsConfig }
import org.scalatest.{ BeforeAndAfterEachTestData, FlatSpec, Matchers, TestData }
import org.slf4j.LoggerFactory

import cats.data.Xor
import ie.zalando.pipeline.backbone.CountWordsPhase.CountWordsTopLevelInitPhase
import ie.zalando.pipeline.backbone.GenericPhases._
import ie.zalando.pipeline.backbone.IsEvenPhase.IsEvenTopLevelInitPhase
import ie.zalando.pipeline.backbone.Phases.{ TransformationPipelineFailure, TransformationPipelineTimeout }
import ie.zalando.pipeline.backbone.SayHelloPhase.SayHelloTopLevelInitPhase
import ie.zalando.pipeline.backbone.{ Backbone, TestDatum }
import net.manub.embeddedkafka.ConsumerExtensions._
import net.manub.embeddedkafka.EmbeddedKafkaConfig
import net.manub.embeddedkafka.streams.EmbeddedKafkaStreamsAllInOne

class KafkaStreamsBackboneCoordinatorSpec extends FlatSpec with Matchers with BeforeAndAfterEachTestData with EmbeddedKafkaStreamsAllInOne {

  import KafkaStreamsTestDefs._

  private var OUTPUT_TOPIC: String = "test-topic-output-"

  private var INPUT_TOPIC: String = "test-topic-input-"

  private val random = new scala.util.Random(54321L)
  private val randomInt = 6000 + random.nextInt(1000)

  implicit val embeddedKafkaConfig = EmbeddedKafkaConfig(zooKeeperPort = 6001, kafkaPort = 6928,
    customBrokerProperties = Map("offsets.topic.num.partitions" -> "1", "offsets.topic.replication.factor" -> "1"))

  implicit val stringSerde = Serdes.String()
  implicit val (stringSerializer, stringDeserializer) = (stringSerde.serializer(), stringSerde.deserializer())

  override protected def beforeEach(testData: TestData): Unit = {
    super.beforeEach(testData)
    val name = testData.name.replaceAll("""\W+""", "-")
    INPUT_TOPIC = "test-topic-input-" + name
    OUTPUT_TOPIC = "test-topic-output-" + name
  }

  val driverInitPhases = List(SayHelloTopLevelInitPhase(), CountWordsTopLevelInitPhase(), IsEvenTopLevelInitPhase())
  val backbone = Backbone[TestDatum](driverInitPhases)

  "A simple streams backbone" should "be possible" in {
    val (builder, _) = configureStreams(KafkaStreamsBackboneCoordinator(backbone))
    runStreamsWithStringConsumer(Seq(INPUT_TOPIC, OUTPUT_TOPIC), builder) { consumer =>
      log.info("Sending messages")
      publishToKafka(INPUT_TOPIC, "Megatron")
      publishToKafka(INPUT_TOPIC, "Soundwave")
      publishToKafka(INPUT_TOPIC, "Shockwave")
      log.info("Checking messages")
      val msgs = consumer.consumeLazily(OUTPUT_TOPIC).take(3).map(_._2).toSet
      msgs should contain("Hello, Megatron, this was calculated on partition 0")
      msgs should contain("Hello, Soundwave, this was calculated on partition 0")
      msgs should contain("Hello, Shockwave, this was calculated on partition 0")
    }
  }

  "A streams backbone with timeout" should "timeout if processing takes too long but not crash the pipeline" in {
    val sleepyBackbone = Backbone[TestDatum](List(xform[TestDatum]((datum: TestDatum) => {
      if (datum.name.startsWith("M"))
        Thread.sleep(10000)
      datum
    }), SayHelloTopLevelInitPhase()))
    val (builder, xformStream) = configureStreams(KafkaStreamsBackboneCoordinator(sleepyBackbone, 250 millis))
    val failed = mutable.Set.empty[String]
    xformStream.foreach(new ForeachAction[String, Xor[TransformationPipelineFailure, TestDatum]] {
      override def apply(key: String, value: Xor[TransformationPipelineFailure, TestDatum]): Unit = {
        log.debug(s"checking value $value")
        value match {
          case Xor.Left(TransformationPipelineTimeout(td: TestDatum, _)) => failed.add(td.name)
          case _ =>
        }
      }
    })
    runStreamsWithStringConsumer(Seq(INPUT_TOPIC, OUTPUT_TOPIC), builder) { consumer =>
      log.info("Sending messages")
      publishToKafka(INPUT_TOPIC, "Megatron")
      publishToKafka(INPUT_TOPIC, "Soundwave")
      log.info("Checking messages")
      val msgs = consumer.consumeLazily(OUTPUT_TOPIC).take(1).map(_._2).toSet
      msgs should contain("Hello, Soundwave, this was calculated on partition 0")
    }
    failed shouldBe mutable.Set("Megatron")
  }

  "A streams backbone" should "use multiple threads for multiple partitions" in {
    withRunningKafka {
      log.info(s"Explicitly creating topic $INPUT_TOPIC")
      createCustomTopic(INPUT_TOPIC, Map.empty, 3, 1)
      createCustomTopic(OUTPUT_TOPIC)
      log.info("Creating streams")
      val (builder, _) = configureStreams(KafkaStreamsBackboneCoordinator(backbone))
      val streams = makeStreams(builder, 3)
      val kafkaProducer = aKafkaProducer[String]
      log.info("Starting kstreams")
      streams.start()
      try {
        withStringConsumer { consumer =>
          log.info("Sending messages")

          val s1 = kafkaProducer.send(new ProducerRecord(INPUT_TOPIC, 0, null, null, "Megatron"))
          val s2 = kafkaProducer.send(new ProducerRecord(INPUT_TOPIC, 1, null, null, "Soundwave"))
          val s3 = kafkaProducer.send(new ProducerRecord(INPUT_TOPIC, 2, null, null, "Shockwave"))

          log.info("Checking messages")
          val msgs = consumer.consumeLazily(OUTPUT_TOPIC).take(3).map(_._2).toSet
          msgs should contain("Hello, Megatron, this was calculated on partition 0")
          msgs should contain("Hello, Soundwave, this was calculated on partition 1")
          msgs should contain("Hello, Shockwave, this was calculated on partition 2")
        }
      } finally {
        streams.close()
      }
    }
  }

  def configureStreams(coordinator: KafkaStreamsBackboneCoordinator[TestDatum]): (KStreamBuilder, KStream[String, Xor[TransformationPipelineFailure, TestDatum]]) = {
    val tdSerde: Serde[TestDatum] = Serdes.serdeFrom(new TestDatumPhraseSerializer(), new TestDatumNameDeserializer())
    val builder = new KStreamBuilder()
    val kstream = builder.stream(stringSerde, tdSerde, INPUT_TOPIC)
    val xformStream = kstream.transformValues[Xor[TransformationPipelineFailure, TestDatum]](coordinator)
    xformStream.flatMapValues(new KafkaStreamValidDatumValueMapper[TestDatum])
      .to(stringSerde, tdSerde, OUTPUT_TOPIC)
    (builder, xformStream)
  }

  def makeStreams(builder: KStreamBuilder, nthreads: Int) =
    new KafkaStreams(builder, new StreamsConfig(backboneStreamProps(nthreads)))

  def backboneStreamProps(nthreads: Int = 1) = {
    val props = new Properties()
    props.put(StreamsConfig.CLIENT_ID_CONFIG, "Backbone-Test-Processor")
    props.put("group.id", "backbone-test")
    props.put(StreamsConfig.APPLICATION_ID_CONFIG, "backbone_test_id")
    props.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, s"localhost:${embeddedKafkaConfig.kafkaPort}")
    props.put(StreamsConfig.REPLICATION_FACTOR_CONFIG, new java.lang.Integer(1))
    props.put(StreamsConfig.TIMESTAMP_EXTRACTOR_CLASS_CONFIG, classOf[WallclockTimestampExtractor])
    props.put(StreamsConfig.NUM_STREAM_THREADS_CONFIG, new java.lang.Integer(nthreads))
    props
  }

}

private object KafkaStreamsTestDefs {

  val log = LoggerFactory.getLogger(getClass)

  case class TestDatumPhraseSerializer() extends Serializer[TestDatum] {
    override def configure(configs: util.Map[String, _], isKey: Boolean): Unit = {
      log.debug(s"$this being configured with $configs isKey: $isKey")
    }

    override def close(): Unit = {
      log.debug(s"$this being closed")
    }

    override def serialize(topic: String, data: TestDatum): Array[Byte] = {
      log.debug(s"$this serializing to topic: $topic $data")
      data.phrase.toString.getBytes
    }

  }

  case class TestDatumNameDeserializer() extends Deserializer[TestDatum] {
    override def configure(configs: util.Map[String, _], isKey: Boolean): Unit = {
      log.debug(s"$this being configured with $configs isKey: $isKey")
    }

    override def close(): Unit = {
      log.debug(s"$this being closed")
    }

    override def deserialize(topic: String, data: Array[Byte]): TestDatum = {
      val s = new String(data)
      log.debug(s"$this deserializing from $topic: $s")
      TestDatum(name = s)
    }
  }

  // Arrg so annoying have to copy-n-paste from EmbeddedKafka
  def baseProducerConfig(implicit config: EmbeddedKafkaConfig) = {
    val props = new Properties()
    props.setProperty(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, s"localhost:${config.kafkaPort}")
    props.setProperty(ProducerConfig.MAX_BLOCK_MS_CONFIG, 5000.toString)
    props.setProperty(ProducerConfig.RETRY_BACKOFF_MS_CONFIG, 1000.toString)
    props
  }

}
