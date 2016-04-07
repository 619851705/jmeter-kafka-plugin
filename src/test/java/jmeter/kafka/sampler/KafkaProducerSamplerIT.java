package jmeter.kafka.sampler;

import static org.mockito.Mockito.doReturn;
import static org.mockito.Mockito.spy;

import java.util.concurrent.atomic.AtomicInteger;

import org.junit.Before;
import org.junit.Test;

public class KafkaProducerSamplerIT {

	private static final int MESSAGES_COUNT = 10;

	private static final String KAFKA_BROKERS = "localhost:9092";
	private static final String KAFKA_TOPIC = "TEST";

	private KafkaProducerSampler sampler = spy(new KafkaProducerSampler());
	private AtomicInteger counter = new AtomicInteger(1);

	@Before
	public void before() {
		doReturn(KAFKA_BROKERS).when(sampler).getKafkaBrokers();
		doReturn(KAFKA_TOPIC).when(sampler).getKafkaTopic();
		doReturn(Integer.toString(counter.getAndIncrement())).when(sampler).getKafkaKey();
	}

	//C:\app\kafka_2.10-0.8.2.2\bin\windows\kafka-console-consumer.bat --zookeeper localhost:2181 --topic TEST
	@Test
	public void test() throws Exception {
		System.out.println("Sending data...");
		for (int x = 0; x < MESSAGES_COUNT; x++) {

			final String message = "{" +
					"\"txnId\":1512083021657," +
					"\"gameId\":1512083024," +
					"\"originId\":1," +
					"\"accId\":" + System.currentTimeMillis() + x + "," +
					"\"accNum\":\"178973\"," +
					"\"username\":\"ALISTAIRF\"," +
					"\"gpId\":1512083024," +
					"\"gpDesc\":\"Game Play for 1512083024\"," +
					"\"gpCreated\":\"2015-12-03T08:30:22.311+0100\"," +
					"\"gpTotStake\":5.0," +
					"\"gpTotWin\":0.0" +
					"}";

			System.out.println(" - " + message);

			doReturn(message).when(sampler).getRequestData();

			sampler.sample(null);
		}
		System.out.println("done !!!");
	}

}
