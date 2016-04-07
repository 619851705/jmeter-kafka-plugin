package jmeter.kafka.sampler;

import java.util.Arrays;
import java.util.Properties;

import org.apache.commons.lang3.StringUtils;
import org.apache.jmeter.samplers.Entry;
import org.apache.jmeter.samplers.SampleResult;
import org.apache.jorphan.logging.LoggingManager;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.apache.log.Logger;

public class KafkaConsumerSampler extends AbstractKafkaSampler {

	private static final Logger logger = LoggingManager.getLoggerForClass();

	public static String KAFKA_BROKERS = "Kafka brokers";
	public static String KAFKA_TOPIC = "Kafka topic";
	public static String GROUP_ID = "Group ID";
	public static String MARKER = "Marker";

	private static int STREAM_COUNT = 1;

	private KafkaConsumer<String, String> consumer;

	private boolean interrupted = false;

	@Override
	public boolean interrupt() {
		logger.info("Interrumping " + getName() + "(" + getSamplerId() + ")");
		interrupted = true;
		return true;
	}

	@Override
	public SampleResult sample(Entry entry) {
		SampleResult result = newSampleResult();
		sampleResultStart(result, null);

		try {
			String marker = getMarker();
			String message = null;
			boolean firstPool = true;
			while (!interrupted && message == null) {
				ConsumerRecords<String, String> records = getConsumer().poll(100);
				if (firstPool) {
					logger.info(getSamplerId() + " starting to consume data...");
					firstPool = false;
				}
				//logger.debug("records.count: " + records.count());
				for (ConsumerRecord<String, String> record : records) {
					String recordValue = record.value();
					logger.debug("Message: " + recordValue);
					if (StringUtils.isBlank(marker) || recordValue.contains(marker) || recordValue.matches(marker)) {
						logger.debug("Message matching marker=" + marker);
						message = recordValue;
						break;
					}
				}
			}
			sampleResultSuccess(result, message);
		} catch (Exception e) {
			logger.error("Failed consuming kafka message", e);
			sampleResultFailed(result, "500", e);
		}

		return result;
	}

	private String getSamplerId() {
		return getGroupId() + "-" + Thread.currentThread().getId();
	}

	private KafkaConsumer<String, String> getConsumer() {
		if (consumer == null) {
			Properties props = new Properties();
			props.put("group.id", getSamplerId());
			props.put("key.deserializer", StringDeserializer.class.getName());
			props.put("value.deserializer", StringDeserializer.class.getName());
			props.put("bootstrap.servers", getKafkaBrokers());
			consumer = new KafkaConsumer<String, String>(props);
			consumer.subscribe(Arrays.asList(getKafkaTopic()));
			logger.info("Connecting to topic=" + getKafkaTopic() + ", group=" + getGroupId() + " and kafka brokers=" + getKafkaBrokers());
		}

		return consumer;
	}

	public void setKafkaBrokers(String text) {
		setProperty(KAFKA_BROKERS, text);
	}

	public String getKafkaBrokers() {
		return getPropertyAsString(KAFKA_BROKERS);
	}

	public void setGroupId(String text) {
		setProperty(GROUP_ID, text);
	}

	public String getGroupId() {
		return getPropertyAsString(GROUP_ID);
	}

	public void setKafkaTopic(String text) {
		setProperty(KAFKA_TOPIC, text);
	}

	public String getKafkaTopic() {
		return getPropertyAsString(KAFKA_TOPIC);
	}

	public void setMarker(String text) {
		setProperty(MARKER, text);
	}

	public String getMarker() {
		return getPropertyAsString(MARKER);
	}

}
