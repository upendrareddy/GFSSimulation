public class ServerAndFreeSpace implements Comparable<Object>{

	private String serverName;
	private int freeSpace;
	
	public ServerAndFreeSpace(String serverName, int freeSpace) {
		this.serverName = serverName;
		this.freeSpace = freeSpace;
	}
	
	public String getServerName() {
		return serverName;
	}
	public void setServerName(String serverName) {
		this.serverName = serverName;
	}
	public int getFreeSpace() {
		return freeSpace;
	}
	public void setFreeSpace(int freeSpace) {
		this.freeSpace = freeSpace;
	}
	
	public String toString(){
		return serverName+" "+freeSpace;
	}
	public int compareTo(Object arg0) {
		ServerAndFreeSpace input = (ServerAndFreeSpace) arg0;
        return (this.freeSpace > input.getFreeSpace()) ? -1: (this.freeSpace < input.getFreeSpace() ) ? 1:0 ;
	}
	
}
