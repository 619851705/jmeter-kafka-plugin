package jmeter.kafka.sampler;

import java.awt.BorderLayout;

import javax.swing.BorderFactory;
import javax.swing.JTextField;

import org.apache.jmeter.gui.util.VerticalPanel;
import org.apache.jmeter.testelement.TestElement;

public class KafkaConsumerSamplerGui extends AbstractKafkaSamplerGui {

	private static final long serialVersionUID = 1L;

	private JTextField kafkaBrokers;
	private JTextField kafkaTopic;
	private JTextField groupId;
	private JTextField marker;

	public KafkaConsumerSamplerGui() {
		init();
	}

	@Override
	public String getStaticLabel() {
		return "Kafka Consumer";
	}

	@Override
	public void configure(TestElement element) {
		super.configure(element);

		this.kafkaBrokers.setText(element.getPropertyAsString(KafkaConsumerSampler.KAFKA_BROKERS));
		this.kafkaTopic.setText(element.getPropertyAsString(KafkaConsumerSampler.KAFKA_TOPIC));
		this.groupId.setText(element.getPropertyAsString(KafkaConsumerSampler.GROUP_ID));
		this.marker.setText(element.getPropertyAsString(KafkaConsumerSampler.MARKER));
	}

	@Override
	public TestElement createTestElement() {
		KafkaConsumerSampler sampler = new KafkaConsumerSampler();
		modifyTestElement(sampler);
		return sampler;
	}

	@Override
	public void modifyTestElement(TestElement sampler) {
		super.configureTestElement(sampler);

		if ((sampler instanceof KafkaConsumerSampler)) {
			KafkaConsumerSampler kafkaSampler = (KafkaConsumerSampler) sampler;
			kafkaSampler.setKafkaBrokers(this.kafkaBrokers.getText());
			kafkaSampler.setKafkaTopic(this.kafkaTopic.getText());
			kafkaSampler.setGroupId(this.groupId.getText());
			kafkaSampler.setMarker(this.marker.getText());
		}
	}

	@Override
	public void clearGui() {
		super.clearGui();

		this.kafkaBrokers.setText("");
		this.kafkaTopic.setText("");
		this.groupId.setText("");
		this.marker.setText("");
	}

	private void init() {
		setLayout(new BorderLayout(0, 5));
		setBorder(makeBorder());

		add(makeTitlePanel(), "North");

		VerticalPanel mainPanel = new VerticalPanel();

		this.kafkaBrokers = new JTextField();
		mainPanel.add(labeledField(KafkaConsumerSampler.KAFKA_BROKERS, this.kafkaBrokers));

		this.kafkaTopic = new JTextField();
		mainPanel.add(labeledField(KafkaConsumerSampler.KAFKA_TOPIC, this.kafkaTopic));

		this.groupId = new JTextField();
		mainPanel.add(labeledField(KafkaConsumerSampler.GROUP_ID, this.groupId));

		this.marker = new JTextField();
		mainPanel.add(labeledField(KafkaConsumerSampler.MARKER, this.marker));

		mainPanel.setBorder(BorderFactory.createEtchedBorder());

		add(mainPanel, "Center");
	}

}
