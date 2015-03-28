import java.util.ArrayList;

public class ServerProperties {

	private int freeSpace;
	private long timestamp;
	private ArrayList<String> chunkNames = null;
	//chunk name will begin with * if its a master
	
	public ServerProperties(int size,long timestamp) {
		this.freeSpace = size;
		this.timestamp = timestamp;
	}

	public void addChunk(String chunkToBeAdded){
		if(chunkNames != null){
			this.chunkNames.add(chunkToBeAdded);
		}else{
			this.chunkNames = new ArrayList<String>();
			this.chunkNames.add(chunkToBeAdded);
		}
	}
	
	public void setChunkNames(String chunks){//process to have only names
		if(chunks != null){
			this.chunkNames = new ArrayList<String>();
			String[] names = chunks.split(",");
			for(String n :names){
				this.chunkNames.add(n.split("=")[0]);
			}
			System.out.println("chunks added to server :"+chunkNames.toString());
		}
		else
			System.out.println("chunks empty!!");
	}

	public ArrayList<String> getChunks(){
		return chunkNames;
	}

	public int getFreeSpace(){
		return freeSpace;
	}

	public void setSize(int spaceAvailable){
		this.freeSpace = spaceAvailable;
	}
	public long getTimestamp(){
		return timestamp;
	}

	public void setTimeStamp(long time){
		timestamp = time;
	}

	@Override
	public String toString(){
		String list = MetaServer.convertToString(chunkNames.toString());
		return freeSpace+"|"+timestamp+"|"+list;
	}
}