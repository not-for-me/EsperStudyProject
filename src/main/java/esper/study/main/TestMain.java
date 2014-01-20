package esper.study.main;

import java.util.Random;

import org.apache.log4j.BasicConfigurator;

import com.espertech.esper.client.EPStatement;

import esper.study.event.BaseEvent;
import esper.study.listener.DefaultListener;


public class TestMain {
	
	public static void main(String[] args) {
		//Log4J Configuration 
		BasicConfigurator.configure();
		
		//Set Default Configuration of Esper Engine
		EsperEngine engine = new EsperEngine();
		
		//Register an EPL Statement to Esper Engine
		String epl = "select eventID, dataIntVal from BaseEvent";
		EPStatement stmt = engine.getEngine().getEPAdministrator().createEPL(epl);

		//Register an Listener to Esper Engine
		DefaultListener myListener = new DefaultListener();
		stmt.addListener(myListener);

		//Now Esper Engine is ready for incoming events
		int eventCounter = 1000;
		for(int i = 0; i < eventCounter; i++){
			BaseEvent event = generateBaseEvent(i);
			engine.getEngine().getEPRuntime().sendEvent(event);
			try { Thread.sleep(1000); } 
			catch (InterruptedException e) { e.printStackTrace(); }
		}
	}

	public static BaseEvent generateBaseEvent(int id) {
		Random randNum = new Random();
		int randValue = randNum.nextInt(100);
		BaseEvent newEvent = new BaseEvent(id, randValue);
		return newEvent;
	}
}