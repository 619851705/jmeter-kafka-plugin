package jmeter.kafka.sampler;

import java.awt.BorderLayout;

import javax.swing.BorderFactory;
import javax.swing.JLabel;
import javax.swing.JTextArea;
import javax.swing.JTextField;

import org.apache.jmeter.gui.util.VerticalPanel;
import org.apache.jmeter.testelement.TestElement;

public class KafkaProducerSamplerGui extends AbstractKafkaSamplerGui {

	private static final long serialVersionUID = 1L;

	private JTextField kafkaBrokers;
	private JTextField kafkaTopic;
	private JTextField kafkaKey;
	private JTextArea requestData;

	public KafkaProducerSamplerGui() {
		init();
	}

	@Override
	public String getStaticLabel() {
		return "Kafka Producer";
	}

	@Override
	public void configure(TestElement element) {
		super.configure(element);

		this.kafkaBrokers.setText(element.getPropertyAsString(KafkaProducerSampler.KAFKA_BROKERS));
		this.kafkaTopic.setText(element.getPropertyAsString(KafkaProducerSampler.KAFKA_TOPIC));
		this.kafkaKey.setText(element.getPropertyAsString(KafkaProducerSampler.KAFKA_KEY));
		this.requestData.setText(element.getPropertyAsString(KafkaProducerSampler.REQUEST_DATA));
	}

	@Override
	public TestElement createTestElement() {
		KafkaProducerSampler sampler = new KafkaProducerSampler();
		modifyTestElement(sampler);
		return sampler;
	}

	@Override
	public void modifyTestElement(TestElement sampler) {
		super.configureTestElement(sampler);

		if ((sampler instanceof KafkaProducerSampler)) {
			KafkaProducerSampler kafkaSampler = (KafkaProducerSampler) sampler;
			kafkaSampler.setKafkaBrokers(this.kafkaBrokers.getText());
			kafkaSampler.setKafkaTopic(this.kafkaTopic.getText());
			kafkaSampler.setKafkaKey(this.kafkaKey.getText());
			kafkaSampler.setRequestData(this.requestData.getText());
		}
	}

	@Override
	public void clearGui() {
		super.clearGui();

		this.kafkaBrokers.setText("");
		this.kafkaTopic.setText("");
		this.kafkaKey.setText("");
		this.requestData.setText("");
	}

	private void init() {
		setLayout(new BorderLayout(0, 5));
		setBorder(makeBorder());

		add(makeTitlePanel(), "North");

		VerticalPanel mainPanel = new VerticalPanel();

		this.kafkaBrokers = new JTextField();
		mainPanel.add(labeledField(KafkaProducerSampler.KAFKA_BROKERS, this.kafkaBrokers));

		this.kafkaTopic = new JTextField();
		mainPanel.add(labeledField(KafkaProducerSampler.KAFKA_TOPIC, this.kafkaTopic));

		this.kafkaKey = new JTextField();
		mainPanel.add(labeledField(KafkaProducerSampler.KAFKA_KEY, this.kafkaKey));

		mainPanel.add(new JLabel(KafkaProducerSampler.REQUEST_DATA));
		this.requestData = new JTextArea();
		this.requestData.setRows(30);
		mainPanel.add(this.requestData);

		mainPanel.setBorder(BorderFactory.createEtchedBorder());

		add(mainPanel, "Center");
	}

}
