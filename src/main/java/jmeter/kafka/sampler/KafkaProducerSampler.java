package jmeter.kafka.sampler;

import java.util.Properties;
import java.util.concurrent.TimeUnit;

import org.apache.jmeter.samplers.Entry;
import org.apache.jmeter.samplers.SampleResult;
import org.apache.jorphan.logging.LoggingManager;
import org.apache.kafka.clients.producer.Callback;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;
import org.apache.kafka.common.serialization.StringSerializer;
import org.apache.log.Logger;

public class KafkaProducerSampler extends AbstractKafkaSampler {

	private static final Logger logger = LoggingManager.getLoggerForClass();

	private static final int TIMEOUT = 3000;

	public static String KAFKA_BROKERS = "Kafka brokers";
	public static String KAFKA_TOPIC = "Kafka topic";
	public static String KAFKA_KEY = "Kafka key";
	public static String REQUEST_DATA = "Request Data";

	private static KafkaProducer<String, String> producer;

	@Override
	public boolean interrupt() {
		logger.info("Interrupting " + getName());
		closeProducer();
		return true;
	}

	private void closeProducer() {
		if (producer != null) {
			producer.close();
			producer = null;
		}
	}

	@Override
	public SampleResult sample(Entry entry) {
		SampleResult result = newSampleResult();
		String topic = getKafkaTopic();
		String key = getKafkaKey();
		String message = getRequestData();

		sampleResultStart(result, message);
		try {
			logger.debug("Sending message " + message);
			ProducerRecord<String, String> record = new ProducerRecord<String, String>(topic, key, message);
			getProducer().send(record, createCallback(topic, key, message)).get(TIMEOUT, TimeUnit.MILLISECONDS);
			sampleResultSuccess(result, null);
		} catch (Exception e) {
			logger.error("Failed producing kafka message", e);
			sampleResultFailed(result, "500", e);
		}
		return result;
	}

	private Callback createCallback(final String topic, final String key, final String message) {
		return new Callback() {

			@Override
			public void onCompletion(RecordMetadata metadata, Exception exception) {
				if (exception != null) {
					logger.error("Failed sending message to " + topic + " for key " + key, exception);
				} else {
					logger.debug("Message sent to topic=" + topic + ", partition=" + metadata.partition() + " with offset=" + metadata.offset());
				}

			}

		};
	}

	private KafkaProducer<String, String> getProducer() {
		if (producer == null) {
			Properties props = new Properties();
			props.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, getKafkaBrokers());
			props.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
			props.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
			props.put(ProducerConfig.METADATA_FETCH_TIMEOUT_CONFIG, 3000);
			props.put(ProducerConfig.RECONNECT_BACKOFF_MS_CONFIG, 10000);
			props.put(ProducerConfig.RETRIES_CONFIG, "3");
			props.put(ProducerConfig.ACKS_CONFIG, "1");

			//			props.put("retries", 0);
			//			props.put("batch.size", 16384);
			//			props.put("linger.ms", 1);
			//			props.put("buffer.memory", 33554432);

			producer = new KafkaProducer<String, String>(props);
			logger.info("Producer created for topic=" + getKafkaTopic() + " and brokers=" + getKafkaBrokers());
		}

		return producer;
	}

	//================================================================

	public void setKafkaBrokers(String text) {
		setProperty(KAFKA_BROKERS, text);
	}

	public String getKafkaBrokers() {
		return getPropertyAsString(KAFKA_BROKERS);
	}

	public void setKafkaTopic(String text) {
		setProperty(KAFKA_TOPIC, text);
	}

	public String getKafkaTopic() {
		return getPropertyAsString(KAFKA_TOPIC);
	}

	public void setKafkaKey(String text) {
		setProperty(KAFKA_KEY, text);
	}

	public String getKafkaKey() {
		return getPropertyAsString(KAFKA_KEY);
	}

	public void setRequestData(String text) {
		setProperty(REQUEST_DATA, text);
	}

	public String getRequestData() {
		return getPropertyAsString(REQUEST_DATA);
	}

}
