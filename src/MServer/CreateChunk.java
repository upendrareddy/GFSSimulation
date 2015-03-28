import java.io.PrintWriter;
import java.net.Socket;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Map;
import java.util.Map.Entry;
import java.util.TreeMap;
public class CreateChunk extends Thread{

	private String message = null;
	private Socket clientSocket = null;
	private MServerCommonStuff sharedObj= null;
	public CreateChunk(String message, Socket clientSocket,MServerCommonStuff shareObj){
		this.message = message;
		this.clientSocket = clientSocket;
		this.sharedObj = shareObj;
		this.start();
	}

	public void run(){
		try{
			//ClientCreate|clientname|filename|data.length
			String[] messageSplits = message.split("\\|");
			if(messageSplits.length != 0) {
				if(messageSplits[0].equals("ClientCreate")){
					System.out.println("inside create");
					String filename = messageSplits[2];
					int numberOfServers = (int) Math.ceil(Double.parseDouble(messageSplits[3])/8192);
					TreeMap<String,ArrayList<String>> servers = MetaServer.getLoadBalancedServers(0, numberOfServers,filename,sharedObj);

					for(Entry<String,ArrayList<String>> entry : servers.entrySet()){
						ArrayList<String> serverNames = entry.getValue();
						String chunkName = entry.getKey();
						int lastChunkSizeToBeAppended = 8192;

						if(numberOfServers == Integer.parseInt(chunkName.substring(chunkName.indexOf("_") + 1))){
							//last chunk to be appended for the file 
							lastChunkSizeToBeAppended = Integer.parseInt(messageSplits[3]) - ((numberOfServers-1)*8192);
						}
						for(int i = 0; i < serverNames.size(); i++) {
							synchronized (sharedObj) {
								Map<String,ServerProperties> serverMap = Collections.synchronizedMap(sharedObj.serverInfoMap);
								ServerProperties serverProp = serverMap.get(serverNames.get(i));
								if(serverNames.get(0).equals(serverNames.get(i))) {
									serverProp.addChunk("*"+chunkName);
								} else {
									serverProp.addChunk(chunkName);
								}
								serverProp.setSize(serverProp.getFreeSpace() - lastChunkSizeToBeAppended );
							}
						}
						Map<String,ChunkInfo> chunkMap = Collections.synchronizedMap(sharedObj.chunkInfo);
						synchronized (sharedObj) {
							
							ChunkInfo chunkInfo = new ChunkInfo(serverNames, lastChunkSizeToBeAppended);
							chunkInfo.setMaster(serverNames.get(0));
							chunkMap.put(chunkName, chunkInfo);
						}
					}
					Map<String,ChunkInfo> chunkMap = Collections.synchronizedMap(sharedObj.chunkInfo);
					StringBuffer messageToBeSent = new StringBuffer();
					messageToBeSent.append("Create--");
					for(Entry<String, ArrayList<String>> entry :servers.entrySet()) {
						messageToBeSent.append(entry.getKey() + "=" + chunkMap.get(entry.getKey()).getServersForTheChunk(true) + "|");
					}
					PrintWriter out = new PrintWriter(clientSocket.getOutputStream());
					out.println(messageToBeSent.toString());
					out.flush();
					clientSocket.close();
				}
			}

		}
		catch(Exception e){
			System.out.println("ERROR : Client processing thread.message: "+message);
			e.printStackTrace();
		}
	}
}