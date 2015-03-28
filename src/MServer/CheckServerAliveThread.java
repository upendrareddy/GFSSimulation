import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.PrintWriter;
import java.net.Socket;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Iterator;
import java.util.Map;
import java.util.PriorityQueue;
import java.util.concurrent.ConcurrentHashMap;

public class CheckServerAliveThread extends Thread{

	MServerCommonStuff sharedObj = null;
	public CheckServerAliveThread(MServerCommonStuff sharedObj) {
		this.sharedObj = sharedObj;
		this.start();
	}

	public void run(){
		while(true){
			try{
				System.out.println("check servers thread...");
				Map<String,ChunkInfo> chunkMap = Collections.synchronizedMap(sharedObj.chunkInfo);
				Map<String,ServerProperties> serverMap = Collections.synchronizedMap(sharedObj.serverInfoMap);
				ConcurrentHashMap<String,ServerProperties> failedServersMap = new ConcurrentHashMap<String,ServerProperties>();

				for(Iterator<Map.Entry<String, ServerProperties>> entry = serverMap.entrySet().iterator(); entry.hasNext();){
					Map.Entry<String, ServerProperties> current = entry.next();
					ServerProperties serverPropertiesOfFailedServer = current.getValue();

					if(System.currentTimeMillis() - serverPropertiesOfFailedServer.getTimestamp() >= 15000){
						System.out.println("$$$$ found one dead server !!"+serverPropertiesOfFailedServer.toString());
						String serverNameOfFailedServer = current.getKey();
						/**
						0.find the failed server
						1.loop over each chunk that server contains
							 1.find a substitute server : substitute server should not be one of the replicated servers
							 2.if this failed server is master then make the substitute server master
							 3.send message to substitute server which chunk name and replicated server. along with this notify if its the
							 	master for that chunk
							 4.wait for acknowledgment from the new server
						3.after all chunks have been replaced remove server entry from the numberOfServersMap  
						 */

						ArrayList<String> chunkNamesOfFailedServer = serverPropertiesOfFailedServer.getChunks();
						synchronized (sharedObj) {
							entry.remove();
						}
						for(String chunkName : chunkNamesOfFailedServer){
							boolean master = false;
							ArrayList<String> serverNames = null;
							if(chunkName.startsWith("*")){
								chunkName = chunkName.substring(1);
								master = true;
							}
							synchronized (sharedObj) {
								if(chunkMap.get(chunkName)!= null)
									serverNames = chunkMap.get(chunkName).getServers();
							}

							if(serverNames != null) {
								String substituteServer = getReplacementServerForChunk(serverNames, serverNameOfFailedServer);

								int pos =0;
								for(int i=0;i<serverNames.size();i++){
									if(serverNames.get(i).equals(serverNameOfFailedServer)){
										pos = i;
										System.out.println("pos found!!");
										break;
									}
								}
								serverNames.remove(pos);
								String messageToBeSent = null;
								if(master){
									messageToBeSent = "MServerCreate|"+"*"+chunkName+"|"+MetaServer.convertToString(serverNames.toString()) + "|" + MetaServer.myDomain;
								}else{
									messageToBeSent = "MServerCreate|"+chunkName+"|"+MetaServer.convertToString(serverNames.toString()) + "|" + MetaServer.myDomain;
								}

								Socket newServer = new Socket(substituteServer,6000);
								PrintWriter out = new PrintWriter(newServer.getOutputStream());
								out.println(messageToBeSent);
								out.flush();

								BufferedReader in = new BufferedReader(new InputStreamReader(newServer.getInputStream()));
								String message = null;

								while((message = in.readLine()) == null){
									try {
										Thread.sleep(100);
									} catch (InterruptedException e1) {
										e1.printStackTrace();
									}	
								}
								newServer.close();
								if(!message.startsWith("ACK")) {
									System.out.println("ERROR in chunk creation on new server!!!" + messageToBeSent);
									synchronized (sharedObj) {
										chunkMap.put(chunkName, new ChunkInfo(serverNames, chunkMap.get(chunkName).getSize()));
									}
								}else if(message.startsWith("ACK")){
									serverNames.add(substituteServer);
									ChunkInfo chunk = new ChunkInfo(serverNames, chunkMap.get(chunkName).getSize());
									if(master)
										chunk.setMaster(substituteServer);
									synchronized (sharedObj) {
										chunkMap.put(chunkName, chunk);
									}
								}
							}
						}
						failedServersMap.put(serverNameOfFailedServer, serverPropertiesOfFailedServer);
						System.out.println("all chunks have been replaced.removing this server from mtable...");
					}
				}
				if(failedServersMap != null && !failedServersMap.isEmpty()){
					synchronized (sharedObj) {
						for(String s:failedServersMap.keySet()){
							serverMap.remove(s);
						}
					}
				}
				Thread.sleep(1000);
			}catch(InterruptedException |IOException e){
				e.printStackTrace();
			}
		}
	}

	private String getReplacementServerForChunk(ArrayList<String> serverNames, String serverNameOfFailedServer) {
		PriorityQueue<ServerAndFreeSpace> heap = new PriorityQueue<ServerAndFreeSpace>();
		for(Iterator<Map.Entry<String, ServerProperties>> entry = sharedObj.serverInfoMap.entrySet().iterator(); entry.hasNext();){
			Map.Entry<String, ServerProperties> current = entry.next();
			boolean shouldBeAvoided = false;
			if(serverNameOfFailedServer.equals(current.getKey())) {
				shouldBeAvoided = true;
			}
			if(shouldBeAvoided == false) {
				for(String serverName : serverNames) {			
					if(serverName.equals(current.getKey())) {
						shouldBeAvoided = true;
					}
				}
			}
			if(!shouldBeAvoided) {
				heap.add(new ServerAndFreeSpace(current.getKey(), current.getValue().getFreeSpace()));
			}
		}
		return heap.poll().getServerName();
	}		
}