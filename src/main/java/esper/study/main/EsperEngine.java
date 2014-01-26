package esper.study.main;

import java.io.File;

import com.espertech.esper.client.Configuration;
import com.espertech.esper.client.EPServiceProvider;
import com.espertech.esper.client.EPServiceProviderManager;
import com.espertech.esperio.http.EsperIOHTTPAdapter;
import com.espertech.esperio.http.config.ConfigurationHTTPAdapter;
import com.espertech.esperio.http.config.Request;

import esper.study.event.BaseEvent;

public class EsperEngine {
	private EPServiceProvider engine;
	private static final String ENGINE_URI = "http-io-test";
	private static final String REQUEST_URI = "http://localhost:8888/esper/test";

	
	public EsperEngine () {
		Configuration config = new Configuration();
		config.configure(new File("./src/main/resources/esper.cfg.xml"));
		this.engine = EPServiceProviderManager.getProvider(ENGINE_URI, config);
		////config.addEventType("BaseEvent", BaseEvent.class.getName());
		//this.engine = EPServiceProviderManager.getDefaultProvider(config);
	}
	
	public void startHTTPAdapter() {
		ConfigurationHTTPAdapter adapterConfig = new ConfigurationHTTPAdapter();
		Request request = new Request();
		request.setStream("DataStream");
		request.setUri(REQUEST_URI);
		adapterConfig.getRequests().add(request);
		EsperIOHTTPAdapter httpAdapter = new EsperIOHTTPAdapter(adapterConfig, ENGINE_URI);
		httpAdapter.start();
	}

	
		
	public EPServiceProvider getEngine() {
		return engine;
	}

	public void setEngine(EPServiceProvider engine) {
		this.engine = engine;
	}
}

