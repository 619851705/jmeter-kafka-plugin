package jmeter.kafka.sampler;

import java.awt.Container;

import javax.swing.JComponent;
import javax.swing.JLabel;

import org.apache.jmeter.gui.util.HorizontalPanel;
import org.apache.jmeter.samplers.gui.AbstractSamplerGui;

public abstract class AbstractKafkaSamplerGui extends AbstractSamplerGui {

	@Override
	public String getLabelResource() {
		return getClass().getSimpleName();
	}

	protected Container labeledField(String label, JComponent component) {
		HorizontalPanel hp = new HorizontalPanel();
		hp.add(new JLabel(label));
		hp.add(component);
		return hp;
	}
}
