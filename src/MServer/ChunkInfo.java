import java.util.ArrayList;

public class ChunkInfo {

	private ArrayList<String> servers = null;
	private int size;
	private String masterServer = null;

	public ChunkInfo(ArrayList<String> servers , int size) {
		this.servers = servers;
		this.size = size;
	}

	public ArrayList<String> getServers(){
		return servers;
	}

	public int getSize(){
		return size;
	}

	public void setSize(int size){
		this.size = size;
	}

	public void setMaster(String master){
		masterServer = master;
	}

	public String getMaster(){
		return masterServer;
	}

	public String getServersForTheChunk(boolean flag) {
		StringBuffer result = new StringBuffer();
		for(String server : servers) {
			if(flag){
				if(masterServer.equals(server))
					result.append("*"+server+",");
				else
					result.append(server+",");
			}else if(!masterServer.equals(server)){
				result.append(server+",");
			}
		}
		return result.toString();
	}

	public void setServers(ArrayList<String> servers){
		if(servers.size() == 3) {
			this.servers = servers;
		}
	}
}