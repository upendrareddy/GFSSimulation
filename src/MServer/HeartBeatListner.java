import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.Map;

public class HeartBeatListner extends Thread {
	String message = null;
	MServerCommonStuff sharedObj = null;
	public HeartBeatListner(String message,MServerCommonStuff sharedObj){
		this.sharedObj = sharedObj;
		this.message = message;
		System.out.println("insite thread of heart"+message);
		this.start();
	}	

	public void run(){
		System.out.println("...... heartbeat thread!!"+message);
		String msgArr[] = message.split("\\|");
		/**
		 * msgArr[0] = Heartbeat message
		 * msgArr[1] = server name
		 * msgArr[2] = free space
		 * msgArr[3] = list
		 */
		if(msgArr.length >= 3 && msgArr[1] != null && msgArr[2] != null ){
			/** message format
			 *  chunkname,chunkname 
			 */
			String serverName = msgArr[1];
			int freeSpace = Integer.parseInt(msgArr[2]);
			Map<String,ServerProperties> serverMap = Collections.synchronizedMap(sharedObj.serverInfoMap);

			ServerProperties serverProperties = new ServerProperties(freeSpace, System.currentTimeMillis());
			if(msgArr.length > 3 && msgArr[3] != null)
				serverProperties.setChunkNames(msgArr[3]);

			synchronized (sharedObj) {
				serverMap.put(serverName, serverProperties);
			}
			if(msgArr.length > 3 && msgArr[3] != null){
				

				synchronized (sharedObj) {
					Map<String, ChunkInfo> chunkAndServerInfo = Collections.synchronizedMap(sharedObj.chunkInfo);
					String[] serverChunkList = msgArr[3].split(",");
					String chunkName = null;
					int size = 0;
					for(String chunkAndSize:serverChunkList) {
						chunkName = chunkAndSize.split("=")[0];
						boolean master = false;
						if(chunkName.startsWith("*")){
							master = true;
							chunkName = chunkName.substring(1);
						}
						size = Integer.parseInt(chunkAndSize.split("=")[1]);
						if(chunkAndServerInfo.containsKey(chunkName)) {
							ArrayList<String> serversList = chunkAndServerInfo.get(chunkName).getServers();
							boolean serverAlreadyPresent = false;
							for(String server : serversList) {
								if(server.equals(serverName)) {
									serverAlreadyPresent = true;
									break;
								}
							}
							if(!serverAlreadyPresent){
								serversList.add(serverName);
								ChunkInfo chunk =  new ChunkInfo(serversList, size);
								if(master)
									chunk.setMaster(serverName);
								chunkAndServerInfo.put(chunkName,chunk);
							}else if(serverAlreadyPresent){
//								ChunkInfo chunk =  chunkAndServerInfo.get(chunkName);
//								chunk.setSize(size);
//								chunkAndServerInfo.put(chunkName,chunk);
							}
						} else {
							ChunkInfo chunk = new ChunkInfo(new ArrayList<String>(Arrays.asList(serverName)), size);
							if(master)
								chunk.setMaster(serverName);
							chunkAndServerInfo.put(chunkName,chunk);
						}
					}
				}
			}
		}
	}
}