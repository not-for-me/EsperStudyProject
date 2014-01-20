package esper.study.listener;

import com.espertech.esper.client.EventBean;
import com.espertech.esper.client.UpdateListener;

public class DefaultListener implements UpdateListener {
	
	public void update(EventBean[] newEvents, EventBean[] oldEvents) {
		if(newEvents != null){
			for(EventBean event : newEvents){
				System.out.println("ID: " + event.get("eventID") + " Int Value: " + event.get("dataIntVal"));
			}
		}
	}
}

