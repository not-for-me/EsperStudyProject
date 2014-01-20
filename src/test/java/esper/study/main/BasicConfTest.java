package esper.study.main;

import java.util.Random;

import org.junit.Before;
import org.junit.Test;

import static org.junit.Assert.*;

import com.espertech.esper.client.EPStatement;
import com.espertech.esper.client.EventBean;
import com.espertech.esper.client.SafeIterator;

import esper.study.event.BaseEvent;

public class BasicConfTest {
	private EPStatement stmt;
	private final int SLEEP_SEC = 1;
	private final int TEST_EVENT_NUM = 10;
	
	@Before
	public void setUp() {
		//Set Default Configuration of Esper Engine
		EsperEngine engine = new EsperEngine();

		//Register an EPL Statement to Esper Engine
		String epl = "select eventID, dataIntVal from BaseEvent.win:time("+ TEST_EVENT_NUM + 1 +" sec)";
		this.stmt = engine.getEngine().getEPAdministrator().createEPL(epl);

		//Now Esper Engine is ready for incoming events
		int eventCounter = TEST_EVENT_NUM;
		for(int i = 0; i < eventCounter; i++){
			BaseEvent event = generateBaseEvent(i);
			System.out.println("[Test Input] Id: " + event.getEventID() + " Value: " + event.getDataIntVal());
			engine.getEngine().getEPRuntime().sendEvent(event);
			try { Thread.sleep(SLEEP_SEC * 1000); } 
			catch (InterruptedException e) { e.printStackTrace(); }
		}
	}
	
	@Test
	public void matchEventID() {
		SafeIterator<EventBean> safeIter = stmt.safeIterator();
		try{
			for(int i=0;safeIter.hasNext();i++) {
				EventBean event = safeIter.next();
				System.out.println("ID: " + event.get("eventID") + " Int Value: " + event.get("dataIntVal"));
				int resultID = (Integer) event.get("eventID");
				assertEquals(resultID, i);
			}
		}
		finally{
			safeIter.close();
		}
	}

	public static BaseEvent generateBaseEvent(int id) {
		Random randNum = new Random();
		int randValue = randNum.nextInt(100);
		BaseEvent newEvent = new BaseEvent(id, randValue);
		return newEvent;
	}

}
