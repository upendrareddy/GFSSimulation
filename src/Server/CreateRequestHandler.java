import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.FileWriter;
import java.io.InputStreamReader;
import java.io.PrintWriter;
import java.net.Socket;

public class CreateRequestHandler extends Thread{

	String request = null;
	Socket socket = null;

	public CreateRequestHandler(String message, Socket socket){
		this.request = message;
		this.socket = socket;
		System.out.println("###"+message);
		start();
	}

	public void run() {
		try{
			if(request != null) {
				System.out.println("request "+request);
				String[] arr = request.split("\\|");
				//server : ClientCreate|clientname|s1,s2|chunkname|data

				if(arr[0].equals("ClientCreate")) {
					String chunkName = arr[3];
					if(chunkName.startsWith("*")){
						chunkName = chunkName.substring(1);
						ServerMain.chunksHavingMeAsMasterServer.put(chunkName, 1);
					}
					System.out.println("chunk name:"+chunkName);
					String data = arr[4];
					int ackCount = 0;
					String messageToSecondaryServers = "ServerCreate|" + ServerMain.myDomain + "|" + chunkName + "|" + data;
					PrintWriter clientOutServer = null;
					Socket secondarySocketOne = null;
					Socket secondarySocketTwo = null;
					PrintWriter outServerOne = null;						
					BufferedReader inServerOne = null;
					PrintWriter outServerTwo = null;
					BufferedReader inServerTwo = null;
					//Secondary Server One
					try {
						secondarySocketOne = new Socket(arr[2].split(",")[0], 6000);
						outServerOne = new PrintWriter(secondarySocketOne.getOutputStream());	
						inServerOne = new BufferedReader(new InputStreamReader(secondarySocketOne.getInputStream()));
						outServerOne.println(messageToSecondaryServers);
						outServerOne.flush();
						System.out.println("sent data to "+arr[2].split(",")[0]);
						String replyFromPrimaryServer = null;
						while((replyFromPrimaryServer = inServerOne.readLine()) == null){
							try {
								Thread.sleep(100);
								System.out.println("inside sleep of sec 1");
							} catch (InterruptedException e1) {
								e1.printStackTrace();
								System.out.println("Failed to create first phase. Server unavailable " + secondarySocketOne);
								secondarySocketOne.close();
								return;
								//break;
							}        
						}
						System.out.println(replyFromPrimaryServer+" response sec1");
						if(replyFromPrimaryServer != null && replyFromPrimaryServer.startsWith("ACK")) {
							System.out.println(arr[2].split(",")[0] + " create first phase success!!!!!");	
							ackCount++;
						} else {
							clientOutServer = new PrintWriter(socket.getOutputStream());		
							clientOutServer.println("Failed to create first phase. Server unavailable" + secondarySocketOne);
							clientOutServer.flush();
							socket.close();
							secondarySocketOne.close();
							return;
						}
					} catch(Exception ex) {
						ex.printStackTrace();
						clientOutServer = new PrintWriter(socket.getOutputStream());		
						clientOutServer.println("Failed to create first phase. Server unavailable" + secondarySocketOne);
						clientOutServer.flush();
						socket.close();
						secondarySocketOne.close();
						return;
					}

					/*
					 * secondary server two
					 */
					try {
						secondarySocketTwo = new Socket(arr[2].split(",")[1], 6000);
						outServerTwo = new PrintWriter(secondarySocketTwo.getOutputStream());	
						inServerTwo = new BufferedReader(new InputStreamReader(secondarySocketTwo.getInputStream()));
						outServerTwo.println(messageToSecondaryServers);
						outServerTwo.flush();
						System.out.println("#%@sent to "+arr[2].split(",")[1]);
						String replyFromPrimaryServer = null;
						while((replyFromPrimaryServer = inServerTwo.readLine()) == null){
							try {
								Thread.sleep(100);
								System.out.println("22222222222222");
							} catch (InterruptedException e1) {
								System.out.println("Failed to create first phase. Server unavailable " + secondarySocketTwo);
								messageToSecondaryServers = "ServerNoCommit|" + ServerMain.myDomain + "|" + chunkName + "|" + data;
								outServerOne.println(messageToSecondaryServers);
								outServerOne.flush();
								e1.printStackTrace();
								clientOutServer = new PrintWriter(socket.getOutputStream());		
								clientOutServer.println("Failed to create first phase. Server unavailable" + secondarySocketTwo);
								clientOutServer.flush();
								socket.close();
								secondarySocketOne.close();
								secondarySocketTwo.close();
								return;
								//break;
							}        
						}
						System.out.println("msg 222222222 "+replyFromPrimaryServer);
						if(replyFromPrimaryServer != null && replyFromPrimaryServer.startsWith("ACK")) {
							System.out.println(arr[2].split(",")[1] + " create first phase success!!!!!");	
							ackCount++;
						} else {
							messageToSecondaryServers = "ServerNoCommit|" + ServerMain.myDomain + "|" + chunkName + "|" + data;
							outServerOne.println(messageToSecondaryServers);
							outServerOne.flush();							
							clientOutServer = new PrintWriter(socket.getOutputStream());		
							clientOutServer.println("Failed to create first phase. Server unavailable " + ServerMain.myDomain);
							clientOutServer.flush();
							socket.close();
							secondarySocketOne.close();
							secondarySocketTwo.close();
							return;
						}
					} catch(Exception ex) {
						ex.printStackTrace();
						messageToSecondaryServers = "ServerNoCommit|" + ServerMain.myDomain + "|" + chunkName + "|" + data;
						outServerOne.println(messageToSecondaryServers);
						outServerOne.flush();							
						clientOutServer = new PrintWriter(socket.getOutputStream());		
						clientOutServer.println("Failed to create first phase. Server unavailable " + ServerMain.myDomain);
						clientOutServer.flush();
						socket.close();
						secondarySocketOne.close();
						secondarySocketTwo.close();
						return;
					}

					if(ackCount == 2) {
						System.out.println("ACKS!!!!!!!!!!!!!!!!!!!!!");
						messageToSecondaryServers = "ServerCommit|" + ServerMain.myDomain + "|" + chunkName + "|" + data;
						try {
							outServerOne.println(messageToSecondaryServers);
							outServerOne.flush();
							secondarySocketOne.close();
							outServerTwo.println(messageToSecondaryServers);
							outServerTwo.flush();
							secondarySocketTwo.close();
						} catch (Exception ex) {
							System.out.println("ServerCreate Failed create phase. Failed to connect to the secondary server");	
							ex.printStackTrace();
							clientOutServer = new PrintWriter(socket.getOutputStream());		
							clientOutServer.println("Server create Failed second phase. Server unavailable");
							clientOutServer.flush();
							socket.close();
							return;
							//break;
						}

						//File file =new File("ChunkFolder_"+ServerMain.id+"/"+chunkName);
						FileWriter fileWritter = null;
						fileWritter = new FileWriter("ChunkFolder_"+ServerMain.id+"/"+chunkName);
						BufferedWriter bufferWritter = new BufferedWriter(fileWritter);
						bufferWritter.write(data);
						bufferWritter.close();
						clientOutServer = new PrintWriter(socket.getOutputStream());		
						clientOutServer.println("ACK");
						clientOutServer.flush();
						socket.close();
						secondarySocketOne.close();
						secondarySocketTwo.close();
					}
					//ServerAppend
				} else if(arr[0].equals("ServerCreate")) {//Still doubtful
					System.out.println("inside server#@$#@$#%#$");
					String serverName = arr[1];
					String chunkName = arr[2];
					String data = arr[3];

					PrintWriter outServer = new PrintWriter(socket.getOutputStream());
					BufferedReader inReader = new BufferedReader(new InputStreamReader(socket.getInputStream()));
					outServer.println("ACK");
					outServer.flush();
					String messageFromPrimary = null;
					while((messageFromPrimary = inReader.readLine()) == null){
						try {
							Thread.sleep(100);
						} catch (InterruptedException e1) {
							System.out.println("Create failed.Error in secondary server"+ServerMain.mServerDomain);
						}        
					}
					if(messageFromPrimary.startsWith("ServerNoCommit")) {
						System.out.println(serverName + " No commit of the chunk. Create failed.Error in secondary server"+ServerMain.mServerDomain);
					} else if(messageFromPrimary.startsWith("ServerCommit")) {
						FileWriter fileWritter = null;
						fileWritter = new FileWriter("ChunkFolder_"+ServerMain.id+"/"+chunkName);
						BufferedWriter bufferWritter = new BufferedWriter(fileWritter);
						bufferWritter.write(data);
						bufferWritter.close();
					}
				} 
			}
		}catch(Exception e){
			e.printStackTrace();
		}
	}

}