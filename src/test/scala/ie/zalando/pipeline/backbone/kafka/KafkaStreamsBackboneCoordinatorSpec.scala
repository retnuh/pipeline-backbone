package ie.zalando.pipeline.backbone.kafka

import java.util
import java.util.Properties
import java.util.concurrent.{ BlockingQueue, CountDownLatch, LinkedBlockingQueue, TimeUnit }

import org.apache.kafka.clients.producer.{ KafkaProducer, ProducerConfig, ProducerRecord }
import org.apache.kafka.common.serialization.Serdes.StringSerde
import org.apache.kafka.common.serialization.{ Deserializer, Serde, Serdes, Serializer, StringSerializer }
import org.apache.kafka.streams.kstream.{ ForeachAction, KStreamBuilder }
import org.apache.kafka.streams.processor.WallclockTimestampExtractor
import org.apache.kafka.streams.{ KafkaStreams, StreamsConfig }
import org.scalatest.{ BeforeAndAfterEachTestData, FlatSpec, Matchers, TestData }
import org.slf4j.LoggerFactory

import ie.zalando.pipeline.backbone.CountWordsPhase.CountWordsTopLevelInitPhase
import ie.zalando.pipeline.backbone.IsEvenPhase.IsEvenTopLevelInitPhase
import ie.zalando.pipeline.backbone.SayHelloPhase.SayHelloTopLevelInitPhase
import ie.zalando.pipeline.backbone.{ Backbone, TestDatum }
import net.manub.embeddedkafka.{ EmbeddedKafka, EmbeddedKafkaConfig }

class KafkaStreamsBackboneCoordinatorSpec extends FlatSpec with Matchers with BeforeAndAfterEachTestData {

  import KafkaStreamsTestDefs._

  private var OUTPUT_TOPIC: String = "test-topic-output-"

  private var INPUT_TOPIC: String = "test-topic-input-"

  private val random = new scala.util.Random(54321L)
  private val randomInt = 6000 + random.nextInt(1000)

  implicit val embeddedKafkaConfig = EmbeddedKafkaConfig(zooKeeperPort = 6001, kafkaPort = 6928)

  override protected def beforeEach(testData: TestData): Unit = {
    super.beforeEach(testData)
    val name = testData.name.replaceAll("""\W+""", "-")
    INPUT_TOPIC = "test-topic-input-" + name
    OUTPUT_TOPIC = "test-topic-output-" + name
    // It's not ideal that we have to start/stop the server for each test, but the tests don't
    // pass otherwise
    EmbeddedKafka.start()(embeddedKafkaConfig)
  }

  override protected def afterEach(testData: TestData): Unit = {
    try super.afterEach(testData)
    finally EmbeddedKafka.stop()
  }

  val driverInitPhases = List(SayHelloTopLevelInitPhase(), CountWordsTopLevelInitPhase(), IsEvenTopLevelInitPhase())
  val backbone = Backbone[TestDatum](driverInitPhases)

  "A simple streams backbone" should "be possible" in {
    log.info("Creating streams")
    val queue = new LinkedBlockingQueue[String]()
    val latch = new CountDownLatch(1)
    val streams = makeStreams(KafkaStreamsBackboneCoordinator(backbone, Some(latch)), 1, queue)
    try {
      log.info("Starting kstreams")
      streams.start()
      if (!latch.await(40, TimeUnit.SECONDS))
        fail("Streams didn't complete before timeout")
      log.info("Sending messages")
      EmbeddedKafka.publishStringMessageToKafka(INPUT_TOPIC, "Megatron")
      EmbeddedKafka.publishStringMessageToKafka(INPUT_TOPIC, "Soundwave")
      EmbeddedKafka.publishStringMessageToKafka(INPUT_TOPIC, "Shockwave")
      log.info("Checking messages")
      val msgs = new java.util.HashSet[String]()
      msgs.add(queue.poll(40, TimeUnit.SECONDS))
      log.info("First poll complete")
      msgs.add(queue.poll(40, TimeUnit.SECONDS))
      log.info("Second poll complete")
      msgs.add(queue.poll(40, TimeUnit.SECONDS))
      log.info("Third poll complete")
      msgs should contain("Hello, Megatron, this was calculated on partition 0")
      msgs should contain("Hello, Soundwave, this was calculated on partition 0")
      msgs should contain("Hello, Shockwave, this was calculated on partition 0")
    } finally {
      log.info("Stopping kstreams")
      streams.close()
    }
  }

  "A streams backbone" should "use multiple threads for multiple partitions" in {
    log.info(s"Explicitly creating topic $INPUT_TOPIC")
    EmbeddedKafka.createCustomTopic(INPUT_TOPIC, Map.empty, 3, 1)
    log.info("Topic created; sleeping")
    Thread.sleep(1000)
    log.info("Creating streams")
    val queue = new LinkedBlockingQueue[String]()
    val latch = new CountDownLatch(1)
    val streams = makeStreams(KafkaStreamsBackboneCoordinator(backbone, Some(latch)), 3, queue)
    val kafkaProducer = new KafkaProducer(baseProducerConfig, new StringSerializer, new StringSerializer)
    try {
      log.info("Starting kstreams")
      streams.start()
      if (!latch.await(40, TimeUnit.SECONDS))
        fail("Streams didn't complete before timeout")
      log.info("Sending messages")

      val s1 = kafkaProducer.send(new ProducerRecord(INPUT_TOPIC, 0, null, null, "Megatron"))
      val s2 = kafkaProducer.send(new ProducerRecord(INPUT_TOPIC, 1, null, null, "Soundwave"))
      val s3 = kafkaProducer.send(new ProducerRecord(INPUT_TOPIC, 2, null, null, "Shockwave"))

      log.info("Checking messages")
      val msgs = new java.util.HashSet[String]()
      s1.get(40, TimeUnit.SECONDS)
      s2.get(40, TimeUnit.SECONDS)
      s3.get(40, TimeUnit.SECONDS)
      log.info("Futures processed; polling queue")
      msgs.add(queue.poll(40, TimeUnit.SECONDS))
      log.info("First poll complete")
      msgs.add(queue.poll(40, TimeUnit.SECONDS))
      log.info("Second poll complete")
      msgs.add(queue.poll(40, TimeUnit.SECONDS))
      log.info("Third poll complete")
      msgs should contain("Hello, Megatron, this was calculated on partition 0")
      msgs should contain("Hello, Soundwave, this was calculated on partition 1")
      msgs should contain("Hello, Shockwave, this was calculated on partition 2")
    } finally {
      log.info("Stopping kstreams")
      try kafkaProducer.close()
      finally streams.close()
    }
  }

  def makeStreams(coordinator: KafkaStreamsBackboneCoordinator[TestDatum], nthreads: Int, queue: BlockingQueue[String]) = {
    val tdSerde: Serde[TestDatum] = Serdes.serdeFrom(new TestDatumPhraseSerializer(), new TestDatumNameDeserializer())
    val sSerde = new StringSerde()
    val builder = new KStreamBuilder()
    val kstream = builder.stream(sSerde, tdSerde, INPUT_TOPIC)
    kstream.transformValues(coordinator)
      .flatMapValues(new KafkaStreamValidDatumValueMapper[TestDatum])
      .to(sSerde, tdSerde, OUTPUT_TOPIC)
    val qstream = builder.stream(sSerde, sSerde, OUTPUT_TOPIC)
    qstream.foreach(new ForeachAction[String, String] {
      override def apply(key: String, value: String): Unit = queue.put(value)
    })
    new KafkaStreams(builder, new StreamsConfig(backboneStreamProps(nthreads)))
  }

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
