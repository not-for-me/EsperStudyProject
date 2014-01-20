package esper.study.event;

public class UpdateHistory {
	private String firstUpdate;
	private String lastUpdate;
	
	public UpdateHistory(String firstUpdate, String lastUpdate) {
		this.firstUpdate = firstUpdate;
		this.lastUpdate = lastUpdate;
	}
	
	public String getFirstUpdate() {
		return firstUpdate;
	}
	public void setFirstUpdate(String firstUpdate) {
		this.firstUpdate = firstUpdate;
	}
	public String getLastUpdate() {
		return lastUpdate;
	}
	public void setLastUpdate(String lastUpdate) {
		this.lastUpdate = lastUpdate;
	}
}
