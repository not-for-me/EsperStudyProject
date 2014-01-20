package esper.study.main;

import com.espertech.esper.client.Configuration;
import com.espertech.esper.client.EPServiceProvider;
import com.espertech.esper.client.EPServiceProviderManager;

import esper.study.event.BaseEvent;

public class EsperEngine {
	private EPServiceProvider engine;
	
	public EsperEngine () {
		Configuration config = new Configuration();
		config.addEventType("BaseEvent", BaseEvent.class.getName());
		this.engine = EPServiceProviderManager.getDefaultProvider(config);
	}
		
	public EPServiceProvider getEngine() {
		return engine;
	}

	public void setEngine(EPServiceProvider engine) {
		this.engine = engine;
	}
	
}
