package esper.study.listener;

import org.apache.log4j.Logger;

import com.espertech.esper.client.EventBean;
import com.espertech.esper.client.UpdateListener;

public class DefaultListener implements UpdateListener {
	static Logger log = Logger.getLogger(DefaultListener.class);
	
	public void update(EventBean[] newEvents, EventBean[] oldEvents) {
		if(newEvents != null){
			for(EventBean event : newEvents){
				System.out.println("ID: " + event.get("eventID") + " Int Value: " + event.get("dataIntVal"));
			}
		}
	}
}
