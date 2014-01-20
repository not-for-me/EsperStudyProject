package esper.study.main.basic;

import java.util.ArrayList;
import java.util.Random;

import org.junit.Before;
import org.junit.Test;

import static org.junit.Assert.*;

import com.espertech.esper.client.EPStatement;
import com.espertech.esper.client.EventBean;
import com.espertech.esper.client.SafeIterator;

import esper.study.event.BaseEvent;
import esper.study.main.EsperEngine;

public class BasicConfTest {
	private EsperEngine engine;
	private EPStatement stmt;
	private final int SLEEP_Milli_SEC = 50;
	private final int TEST_EVENT_NUM = 10;
	private ArrayList<BaseEvent> testEvent = new ArrayList<BaseEvent>();

	@Before
	public void setUp() {
		//Set Default Configuration of Esper Engine
		this.engine = new EsperEngine();

		/*
		 * Register an EPL Statement to Esper Engine
		 * The reason of "TEST_EVENT_NUM+1" calculation is for retaining whole events until release them without comparison.  
		 */
		String epl = "select eventID, dataIntVal from BaseEvent.win:time("+ (TEST_EVENT_NUM+1) * SLEEP_Milli_SEC +" milliseconds)";
		this.stmt = engine.getEngine().getEPAdministrator().createEPL(epl);

		//Now Esper Engine is ready for incoming events
		for(int i = 0; i < TEST_EVENT_NUM; i++){
			BaseEvent newEvent = generateBaseEvent(i);
			testEvent.add(newEvent);
		}
	}

	@Test
	public void matchEventID() {
		int i = 0;
		//Input Events to Esper Engine
		for(i = 0; i < TEST_EVENT_NUM; i++){
			System.out.println("[Test Input] Id: " + testEvent.get(i).getEventID() + " Value: " + testEvent.get(i).getDataIntVal());
			engine.getEngine().getEPRuntime().sendEvent(testEvent.get(i));
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
				assertEquals(testEvent.get(i).getEventID(), id);
				assertEquals(testEvent.get(i).getDataIntVal(), dataIntVal);
				System.out.println("[Test Result] Id: " + id + " Value: " + dataIntVal);
			}
		}
		finally{
			safeIter.close();
		}
		assertEquals(TEST_EVENT_NUM, i);
	}

	public static BaseEvent generateBaseEvent(int id) {
		Random randNum = new Random();
		int randValue = randNum.nextInt(100);
		BaseEvent newEvent = new BaseEvent(id, randValue);
		return newEvent;
	}

}
