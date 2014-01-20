package esper.study.main.events;

import static org.junit.Assert.assertEquals;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.Map;

import org.junit.Before;
import org.junit.Test;

import com.espertech.esper.client.EPStatement;
import com.espertech.esper.client.EventBean;
import com.espertech.esper.client.SafeIterator;

import esper.study.event.UpdateHistory;
import esper.study.main.EsperEngine;

public class NestedMapEventTypeTest {
	private EsperEngine engine;
	private EPStatement stmt;
	private final int SLEEP_Milli_SEC = 50;
	private final int TEST_EVENT_NUM = 5;
	private ArrayList<Map<String, Object>> testEvent = new ArrayList<Map<String, Object>>();

	@Before
	public void setUp() {
		//Set Default Configuration of Esper Engine
		this.engine = new EsperEngine();

		//Runtime Event Type Configuration 
		Map<String, Object> updatedFieldDef = new HashMap<String, Object>();
		updatedFieldDef.put("name", String.class);
		updatedFieldDef.put("address", String.class);
		updatedFieldDef.put("history", UpdateHistory.class);
		this.engine.getEngine().getEPAdministrator().getConfiguration().addEventType("UpdatedFieldType", updatedFieldDef);

		Map<String, Object> accountUpdateDef = new HashMap<String, Object>();
		accountUpdateDef.put("accountId", int.class);
		accountUpdateDef.put("fields", "UpdatedFieldType"); 
		// the latter can also be: accountUpdateDef.put("fields", updatedFieldDef);
		this.engine.getEngine().getEPAdministrator().getConfiguration().addEventType("AccountUpdate", accountUpdateDef);

		/*
		 * Register an EPL Statement to Esper Engine
		 * The reason of "TEST_EVENT_NUM+1" calculation is for retaining whole events until release them without comparison.  
		 */
		String epl = "select accountId, fields.name, fields.address, fields.history.firstUpdate, fields.history.lastUpdate"
				+ " from AccountUpdate.win:time("+ (TEST_EVENT_NUM+1) * SLEEP_Milli_SEC +" milliseconds)";
		this.stmt = engine.getEngine().getEPAdministrator().createEPL(epl);

		//Now Esper Engine is ready for incoming events
		testEvent = generateNestedMapEvent();
	}

	@Test
	public void matchEventID() {
		int i = 0;
		//Input Events to Esper Engine
		for(i = 0; i < TEST_EVENT_NUM; i++){
			Map<String, Object> updatedFields = (Map<String, Object>) testEvent.get(i).get("fields");
			UpdateHistory updateHistory = (UpdateHistory) updatedFields.get("history");
			System.out.println("[Test Input] Account Id: " + testEvent.get(i).get("accountId") + 
								" Name: " + updatedFields.get("name") +
								" Address: " + updatedFields.get("address") + 
								" First Update: " + updateHistory.getFirstUpdate() +
								" Last Update: " + updateHistory.getLastUpdate());
			
			engine.getEngine().getEPRuntime().sendEvent(testEvent.get(i), "AccountUpdate");
			try { Thread.sleep(SLEEP_Milli_SEC); } 
			catch (InterruptedException e) { e.printStackTrace(); }
		}
		assertEquals(TEST_EVENT_NUM, i);

		SafeIterator<EventBean> safeIter = stmt.safeIterator();
		try{
			for(i=0;safeIter.hasNext();i++) {
				EventBean event = safeIter.next();
				
				int accountID = (Integer) event.get("accountId");
				String name = (String) event.get("fields.name");
				String address = (String) event.get("fields.address");
				String firstUpdate = (String) event.get("fields.history.firstUpdate");
				String lastUpdate = (String) event.get("fields.history.lastUpdate");

				assertEquals(testEvent.get(i).get("accountId"), accountID);
				
				Map<String, Object> updatedFields = (Map<String, Object>) testEvent.get(i).get("fields");
				UpdateHistory updateHistory = (UpdateHistory) updatedFields.get("history");
				
				assertEquals(updatedFields.get("name"), name);
				assertEquals(updatedFields.get("address"), address);
				assertEquals(updateHistory.getFirstUpdate(), firstUpdate);
				assertEquals(updateHistory.getLastUpdate(), lastUpdate);
				
				System.out.println("[Test Result] Account Id: " + accountID + 
						" Name: " + name +
						" Address: " + address + 
						" First Update: " + firstUpdate +
						" Last Update: " + lastUpdate);
			}
		}
		finally{
			safeIter.close();
		}
		assertEquals(TEST_EVENT_NUM, i);
	}

	public static ArrayList<Map<String, Object>> generateNestedMapEvent() {
		ArrayList<Map<String, Object>> testEvent = new ArrayList<Map<String, Object>>();

		//First Event Data
		Map<String, Object> updatedFields = new HashMap<String, Object>();
		updatedFields.put("name", "Woojin Joe");
		updatedFields.put("address", "Test Address 1");
		updatedFields.put("history", new UpdateHistory("20131231", "20140118"));

		Map<String, Object> account = new HashMap<String, Object>();
		account.put("accountId", 0);
		account.put("fields", updatedFields);
		testEvent.add(account);

		//Second Event Data
		updatedFields = new HashMap<String, Object>();
		updatedFields.put("name", "Yunhye Shim");
		updatedFields.put("address", "Test Address 2");
		updatedFields.put("history", new UpdateHistory("20120831", "20130518"));

		account = new HashMap<String, Object>();
		account.put("accountId", 1);
		account.put("fields", updatedFields);
		testEvent.add(account);

		//Third Event Data
		updatedFields = new HashMap<String, Object>();
		updatedFields.put("name", "Taewoo Kim");
		updatedFields.put("address", "3 Test Address");
		updatedFields.put("history", new UpdateHistory("20131116", "20130928"));

		account = new HashMap<String, Object>();
		account.put("accountId", 2);
		account.put("fields", updatedFields);
		testEvent.add(account);

		//Fourth Event Data
		updatedFields = new HashMap<String, Object>();
		updatedFields.put("name", "Doohyung Cho");
		updatedFields.put("address", "Address Test 4");
		updatedFields.put("history", new UpdateHistory("20140105", "20130111"));

		account = new HashMap<String, Object>();
		account.put("accountId", 3);
		account.put("fields", updatedFields);
		testEvent.add(account);

		//Fifth Event Data
		updatedFields = new HashMap<String, Object>();
		updatedFields.put("name", "Hakjun Kim");
		updatedFields.put("address", "Address 555");
		updatedFields.put("history", new UpdateHistory("20130909", "20131024"));

		account = new HashMap<String, Object>();
		account.put("accountId", 4);
		account.put("fields", updatedFields);
		testEvent.add(account);

		return testEvent;
	}
}
