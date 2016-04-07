package jmeter.kafka.sampler;

import static org.mockito.Mockito.doReturn;
import static org.mockito.Mockito.spy;

import java.util.Arrays;
import java.util.Properties;

import org.apache.jmeter.samplers.SampleResult;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.junit.Before;
import org.junit.Test;

public class KafkaConsumerSamplerIT {

	private static final String KAFKA_BROKERS = "localhost:9092";
	private static final String KAFKA_TOPIC = "TEST";
	private static final String GROUP_ID = "TEST-" + System.currentTimeMillis();
	private static final String MARKER = "\"originId\":1";

	private KafkaConsumerSampler sampler = spy(new KafkaConsumerSampler());

	@Before
	public void before() {
		doReturn(KAFKA_BROKERS).when(sampler).getKafkaBrokers();
		doReturn(KAFKA_TOPIC).when(sampler).getKafkaTopic();
		doReturn(GROUP_ID).when(sampler).getGroupId();
		doReturn(MARKER).when(sampler).getMarker();
	}

	@Test
	public void test() throws Exception {
		boolean loop = true;
		while (loop) {
			SampleResult result = sampler.sample(null);
			System.out.println(result.getResponseDataAsString());
		}
	}

	//@Test
	public void test2() throws InterruptedException {
		Properties props = new Properties();
		props.put("bootstrap.servers", KAFKA_BROKERS);
		props.put("group.id", "test");
		//		props.put("enable.auto.commit", "true");
		//		props.put("auto.commit.interval.ms", "1000");
		//		props.put("session.timeout.ms", "30000");

		//props.put("sync.time.ms", "200");

		props.put("key.deserializer", StringDeserializer.class.getName());
		props.put("value.deserializer", StringDeserializer.class.getName());
		KafkaConsumer<String, String> consumer = new KafkaConsumer<String, String>(props);
		consumer.subscribe(Arrays.asList(KAFKA_TOPIC));
		System.out.println("Connecting...");
		boolean connected = false;
		while (true) {
			ConsumerRecords<String, String> records = consumer.poll(100);
			if (!connected) {
				System.out.println("Consuming data...");
				connected = true;
			}
			for (ConsumerRecord<String, String> record : records) {
				System.out.printf("offset = %d, key = %s, value = %s", record.offset(), record.key(), record.value());
				System.out.println("---");
			}
		}
	}
}
