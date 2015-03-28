import java.io.BufferedReader;
import java.io.InputStreamReader;
import java.io.PrintWriter;
import java.net.Socket;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Map;
import java.util.Map.Entry;
import java.util.TreeMap;

public class ProcessAppendsAndReads extends Thread{
	String request = null;
	Socket mySocket = null;
	MServerCommonStuff dataHolder = null;

	public ProcessAppendsAndReads(Socket socket, String message,MServerCommonStuff dataHolder){
		this.dataHolder = dataHolder;
		this.mySocket = socket;
		this.request = message;
		this.start();
	}

	public void run(){
		try{
			if(request != null) {
				if(request.startsWith("ClientAppend")){
					//ClientAppend|clientname|filename|data.length
					synchronized (dataHolder) {
						System.out.println("@#$@#reuqest"+request);
						String[] messageSplits = request.split("\\|");
						String fileName = messageSplits[2];
						int sizeOfFile = Integer.parseInt(messageSplits[3]);
						Map<String,ServerProperties> serverMap = Collections.synchronizedMap(dataHolder.serverInfoMap);
						Map<String,ChunkInfo> chunkMap = Collections.synchronizedMap(dataHolder.chunkInfo);

						String lastChunkNameForAGivenFile = getLastChunkNameForAGivenFile(fileName, dataHolder);
						System.out.println("@#$@#lastChunkNameForAGivenFile,,,,,,,,"+lastChunkNameForAGivenFile);
						int lastChunkSize = 0;

						if(chunkMap.get(lastChunkNameForAGivenFile) != null)
							lastChunkSize = chunkMap.get(lastChunkNameForAGivenFile).getSize();
						System.out.println("@#$@#lastChunkSize :***********"+lastChunkSize);

						String messageToBeSent = null;
						if(lastChunkSize + sizeOfFile <= 8192) {
							System.out.println("@#$@#inside emply#########3");
							ChunkInfo chunkUpdate = chunkMap.get(lastChunkNameForAGivenFile);
							chunkUpdate.setSize(lastChunkSize + sizeOfFile);
							dataHolder.chunkInfo.put(lastChunkNameForAGivenFile, chunkUpdate);
							// Append--servername1,*servername2,servername3|chunkname
							ChunkInfo chunkInfo = chunkMap.get(lastChunkNameForAGivenFile);
							System.out.println("@#$@#.....chunkInfo size"+"lastChunkNameForAGivenFile"+chunkInfo.getSize());
							messageToBeSent = "Append--" +chunkInfo.getServersForTheChunk(true) + "|" + lastChunkNameForAGivenFile;								

						} else {
							System.out.println("@#$@#inside new chunk creation");
							ChunkInfo chunkUpdate = chunkMap.get(lastChunkNameForAGivenFile);
							chunkUpdate.setSize(8192);
							dataHolder.chunkInfo.put(lastChunkNameForAGivenFile, chunkUpdate);
							System.out.println("@#$@#.....chunkInfo size"+lastChunkNameForAGivenFile+"..."+dataHolder.chunkInfo.get(lastChunkNameForAGivenFile).getSize());
							ChunkInfo chunkInfo = chunkMap.get(lastChunkNameForAGivenFile);
							StringBuffer stringBuffer = new StringBuffer();
							String primaryServer = chunkMap.get(lastChunkNameForAGivenFile).getMaster();
							//MServerNullAppend|chunkname|s1,s2|no of nulls
							stringBuffer.append("MServerNullAppend|"+lastChunkNameForAGivenFile+"|"+chunkInfo.getServersForTheChunk(false)
									+"|" + (8192-lastChunkSize));
							System.out.println("@#$@#nulll........");
							Socket serverSocket = null;
							try {
								serverSocket = new Socket(primaryServer, 6000); 
								PrintWriter serverWriter = new PrintWriter(serverSocket.getOutputStream());
								serverWriter.println(stringBuffer.toString());
								serverWriter.flush();

								BufferedReader serverReader = new BufferedReader(new InputStreamReader(serverSocket.getInputStream()));								
								String replyFromPrimaryServer = null;
								while((replyFromPrimaryServer = serverReader.readLine()) == null){
									try {
										Thread.sleep(100);
									} catch (InterruptedException e1) {
										e1.printStackTrace();
										System.out.println("@#$@#Failed to Null append first phase. Server unavailable " + serverSocket);
										serverSocket.close();
										break;
									}        
								}
								if(replyFromPrimaryServer != null && replyFromPrimaryServer.startsWith("ACK")) {
									System.out.println("@#$@#Null append phase success!!!!!");
								}
							} catch (Exception e) {
								serverSocket.close();
								e.printStackTrace();
							}
							int lastChunkIndexForAGivenFile = Integer.parseInt(
									lastChunkNameForAGivenFile.substring(lastChunkNameForAGivenFile.indexOf("_") + 1));
							TreeMap<String,ArrayList<String>> servers = 
									MetaServer.getLoadBalancedServers(lastChunkIndexForAGivenFile, 1, fileName,dataHolder);

							String chunkName = servers.firstEntry().getKey();
							ChunkInfo chunkToBeAdded = new ChunkInfo(servers.firstEntry().getValue(), sizeOfFile);
							chunkToBeAdded.setMaster(servers.firstEntry().getValue().get(0));
							synchronized (chunkMap) {
								chunkMap.put(chunkName, chunkToBeAdded);	
							}
							//create a new chunk get the servers accordingly
							//Update the server map
							for(Entry<String, ServerProperties> entry : serverMap.entrySet()) {
								String serverInMap = entry.getKey();
								for(String name :servers.firstEntry().getValue()){
									if(serverInMap.equals(name)){
										if(entry.getValue() != null){
											if(chunkName.equals(chunkToBeAdded.getMaster()))
												entry.getValue().addChunk("*"+chunkName);
											else
												entry.getValue().addChunk(chunkName);
											entry.getValue().setSize(entry.getValue().getFreeSpace()-8192);
										}
									}
								}
							}
							messageToBeSent = "Append--" + chunkMap.get(chunkName).getServersForTheChunk(true) + "|" + chunkName;
							System.out.println("@#$@#message to client :"+messageToBeSent);
						}
						PrintWriter out = new PrintWriter(mySocket.getOutputStream());
						System.out.println("@#$@#messageToBeSent....."+messageToBeSent);
						//write the map to output stream
						out.println(messageToBeSent);
						out.flush();
						mySocket.close();
					}
				}else if(request.startsWith("ClientRead")){
					//ClientRead|clientname|filename
					String[] messageSplits = request.split("\\|");							
					String fileName = messageSplits[2];
					TreeMap<String,ArrayList<String>> serverSizeMap = new TreeMap<String,ArrayList<String>>();
					/**
					 * Client 'name' Read 'filename'
					 * 
					 * 1.generate a list of all chunks for that filename and return
					 */
					Map<String,ChunkInfo> syncMap = Collections.synchronizedMap(dataHolder.chunkInfo);
					synchronized (syncMap) {
						for(Entry<String,ChunkInfo> entry : syncMap.entrySet()){
							if(entry.getKey().startsWith(fileName)) {
								serverSizeMap.put(entry.getKey(), entry.getValue().getServers());
							}
						}
					}
					if(serverSizeMap.size() == 0){
						PrintWriter out = new PrintWriter(mySocket.getOutputStream());
						out.println("Error: chunks not available");
						out.flush();
						mySocket.close();
						return;
					}
					PrintWriter out = new PrintWriter(mySocket.getOutputStream());
					out.println("Read--"+MetaServer.convertToStringFromMap(serverSizeMap));
					System.out.println("&&&&&&&&&&&&&&&&&&&&&&&&&&Sending to client");
					System.out.println("Read--"+MetaServer.convertToStringFromMap(serverSizeMap));
					out.flush();
					mySocket.close();
				}
			}
		}catch(Exception e){
			e.printStackTrace();
			System.out.println("Error in append thread");
		}
	}

	private String getLastChunkNameForAGivenFile(String fileName,MServerCommonStuff obj ) {		
		int max = -1;
		String chunkName = null;
		int chunkNumber = -1;
		synchronized (obj) {
			for(Entry<String, ChunkInfo> entry : obj.chunkInfo.entrySet()) {
				chunkName = entry.getKey();
				if(chunkName.startsWith(fileName)) {
					chunkNumber = Integer.parseInt(chunkName.substring(chunkName.indexOf("_") + 1));
					if(chunkNumber > max) {
						max = chunkNumber;
					}
				}
			}
		}
		return fileName+"_"+max;
	}

}
