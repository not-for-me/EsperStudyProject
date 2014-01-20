package esper.study.main.events;

import static org.junit.Assert.assertEquals;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.Map;
import java.util.Random;

import org.junit.Before;
import org.junit.Test;

import com.espertech.esper.client.EPStatement;
import com.espertech.esper.client.EventBean;
import com.espertech.esper.client.SafeIterator;

import esper.study.main.EsperEngine;

public class MapEventTypeTest {
	private EsperEngine engine;
	private EPStatement stmt;
	private final int SLEEP_Milli_SEC = 50;
	private final int TEST_EVENT_NUM = 10;
	private ArrayList<Map<String, Object>> testEvent = new ArrayList<Map<String, Object>>();

	@Before
	public void setUp() {
		//Set Default Configuration of Esper Engine
		this.engine = new EsperEngine();

		//Runtime Event Type Configuration 
		Map<String, Object> def = new HashMap<String, Object>();
		def.put("eventID", Integer.class);
		def.put("dataIntVal", Integer.class);
		engine.getEngine().getEPAdministrator().getConfiguration().addEventType("MapEvent", def);

		/*
		String schemaEPL = "create schema MapEvent (eventID int, dataIntVal int)";
		this.stmt = engine.getEngine().getEPAdministrator().createEPL(schemaEPL);
		 */

		/*
		 * Register an EPL Statement to Esper Engine
		 * The reason of "TEST_EVENT_NUM+1" calculation is for retaining whole events until release them without comparison.  
		 */
		String epl = "select eventID, dataIntVal from MapEvent.win:time("+ (TEST_EVENT_NUM+1) * SLEEP_Milli_SEC +" milliseconds)";
		this.stmt = engine.getEngine().getEPAdministrator().createEPL(epl);

		//Now Esper Engine is ready for incoming events
		for(int i = 0; i < TEST_EVENT_NUM; i++){
			Map<String, Object> newEvent = generateMapEvent(i);
			testEvent.add(newEvent);
		}
	}

	@Test
	public void matchEventID() {
		int i = 0;
		//Input Events to Esper Engine
		for(i = 0; i < TEST_EVENT_NUM; i++){
			System.out.println("[Test Input] Id: " + testEvent.get(i).get("eventID") + " Value: " + testEvent.get(i).get("dataIntVal"));
			engine.getEngine().getEPRuntime().sendEvent(testEvent.get(i), "MapEvent");
			try { Thread.sleep(SLEEP_Milli_SEC); } 
			catch (InterruptedException e) { e.printStackTrace(); }
		}
		assertEquals(TEST_EVENT_NUM, i);

		SafeIterator<EventBean> safeIter = stmt.safeIterator();
		try{
			for(i=0;safeIter.hasNext();i++) {
				EventBean event = safeIter.next();
				int id = (Integer) event.get("eventID");
				int dataIntVal = (Integer) event.get("dataIntVal");
				assertEquals(testEvent.get(i).get("eventID"), id);
				assertEquals(testEvent.get(i).get("dataIntVal"), dataIntVal);
				System.out.println("[Test Result] Id: " + id + " Value: " + dataIntVal);
			}
		}
		finally{
			safeIter.close();
		}
		assertEquals(TEST_EVENT_NUM, i);
	}

	public static Map<String, Object> generateMapEvent(int id) {
		Random randNum = new Random();
		int dataIntVal = randNum.nextInt(100);
		Map<String, Object> newEvent = new HashMap<String, Object>();
		newEvent.put("eventID", new Integer(id));
		newEvent.put("dataIntVal", new Integer(dataIntVal));
		return newEvent;
	}
}
