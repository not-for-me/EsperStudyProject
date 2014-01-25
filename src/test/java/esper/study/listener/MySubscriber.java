package esper.study.listener;

public class MySubscriber {
	public void update(int eventID, int dataIntVal){
		System.out.println("Subscriber's Update Method Invocation!");
		System.out.println("Event ID: " + eventID + "Data Value: " + dataIntVal);
	}
}
