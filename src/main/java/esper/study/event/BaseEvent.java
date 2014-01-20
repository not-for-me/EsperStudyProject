package esper.study.event;

public class BaseEvent {
	private int eventID;
	private int dataIntVal;
	
	public BaseEvent(int eventID, int dataIntVal) {
		this.eventID = eventID;
		this.dataIntVal = dataIntVal;
	}
	
	public int getEventID() {
		return eventID;
	}
	public void setEventID(int eventID) {
		this.eventID = eventID;
	}
	public int getDataIntVal() {
		return dataIntVal;
	}
	public void setDataIntVal(int dataIntVal) {
		this.dataIntVal = dataIntVal;
	}
}
