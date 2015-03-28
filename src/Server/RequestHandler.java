import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileWriter;
import java.io.InputStreamReader;
import java.io.PrintWriter;
import java.net.Socket;
import java.util.Arrays;

public class RequestHandler extends Thread{

	String request = null;
	Socket mySocket = null;

	public RequestHandler(String request,Socket mySocket){
		this.request = request;
		this.mySocket = mySocket;
		this.start();
	}

	public void run(){
		try{
			if(request != null) {
				String[] arr = request.split("\\|");
				//server : ClientAppend|clientname|s1,s2|chunkname|data
				if(arr[0].equals("ClientAppend")) {
					System.out.println("111111111 inside ClientAppend " + request);
					String chunkName = arr[3];					
					String data = arr[4];
					int ackCount = 0;
					String messageToSecondaryServers = "ServerAppend|" + ServerMain.myDomain + "|" + chunkName + "|" + data;
					System.out.println("222222222 messageToSecondaryServers " + messageToSecondaryServers);
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
						System.out.println("333333333 successful connection " + arr[2].split(",")[0]);
						String replyFromPrimaryServer = null;
						while((replyFromPrimaryServer = inServerOne.readLine()) == null){
							try {
								Thread.sleep(100);
								System.out.println("44444444 replyFromPrimaryServer " + replyFromPrimaryServer);
							} catch (InterruptedException e1) {
								e1.printStackTrace();
								System.out.println("Failed to append first phase. Server unavailable " + secondarySocketOne);
								secondarySocketOne.close();
								return;
							}        
						}

						if(replyFromPrimaryServer != null && replyFromPrimaryServer.startsWith("ACK")) {
							System.out.println(arr[2].split(",")[0] + " Append first phase success!!!!!" + secondarySocketOne);	
							ackCount++;
						} else {
							clientOutServer = new PrintWriter(mySocket.getOutputStream());		
							clientOutServer.println("Failed to append first phase. Server unavailable" + secondarySocketOne);
							System.out.println("55555555555 Failed to append first phase " + secondarySocketOne);
							clientOutServer.flush();
							mySocket.close();
							secondarySocketOne.close();
							return;
						}
					} catch(Exception ex) {
						ex.printStackTrace();
						clientOutServer = new PrintWriter(mySocket.getOutputStream());		
						clientOutServer.println("Failed to append first phase. Server unavailable" + secondarySocketOne);
						System.out.println("66666666666 Failed to append first phase " + secondarySocketOne);
						clientOutServer.flush();
						mySocket.close();
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
						System.out.println("77777777777 messageToSecondaryServers " + messageToSecondaryServers);
						String replyFromPrimaryServer = null;
						while((replyFromPrimaryServer = inServerTwo.readLine()) == null){
							try {
								Thread.sleep(100);
							} catch (InterruptedException e1) {
								System.out.println("Failed to append first phase. Server unavailable " + secondarySocketTwo);
								messageToSecondaryServers = "ServerNoCommit|" + ServerMain.myDomain + "|" + chunkName + "|" + data;
								outServerOne.println(messageToSecondaryServers);
								outServerOne.flush();
								System.out.println("888888888888 ServerNoCommit " + messageToSecondaryServers);
								e1.printStackTrace();
								clientOutServer = new PrintWriter(mySocket.getOutputStream());		
								clientOutServer.println("Failed to append first phase. Server unavailable");
								System.out.println("9999999999 Failed to append first phase. Server unavailable");
								clientOutServer.flush();
								mySocket.close();
								secondarySocketOne.close();
								secondarySocketTwo.close();
								return;
								//break;
							}        
						}

						if(replyFromPrimaryServer != null && replyFromPrimaryServer.startsWith("ACK")) {
							System.out.println(arr[2].split(",")[1] + " Append first phase success!!!!!");	
							ackCount++;
						} else {
							messageToSecondaryServers = "ServerNoCommit|" + ServerMain.myDomain + "|" + chunkName + "|" + data;
							outServerOne.println(messageToSecondaryServers);
							outServerOne.flush();			
							System.out.println("!!!!!!!!!!!! ServerNoCommit " + messageToSecondaryServers);
							clientOutServer = new PrintWriter(mySocket.getOutputStream());		
							clientOutServer.println("Failed to append first phase. Server unavailable " + ServerMain.myDomain);
							clientOutServer.println("@@@@@@@@@@@@@Failed to append first phase. Server unavailable " + ServerMain.myDomain);
							clientOutServer.flush();
							mySocket.close();
							secondarySocketOne.close();
							secondarySocketTwo.close();
							return;
						}
					} catch(Exception ex) {
						ex.printStackTrace();
						messageToSecondaryServers = "ServerNoCommit|" + ServerMain.myDomain + "|" + chunkName + "|" + data;
						System.out.println("!!!!!!!!!!!! ServerNoCommit " + messageToSecondaryServers);
						outServerOne.println(messageToSecondaryServers);
						outServerOne.flush();							
						clientOutServer = new PrintWriter(mySocket.getOutputStream());		
						clientOutServer.println("Failed to append first phase. Server unavailable " + ServerMain.myDomain);
						clientOutServer.println("##########Failed to append first phase. Server unavailable " + ServerMain.myDomain);
						clientOutServer.flush();
						mySocket.close();
						secondarySocketOne.close();
						secondarySocketTwo.close();
						return;
					}

					if(ackCount == 2) {
						messageToSecondaryServers = "ServerCommit|" + ServerMain.myDomain + "|" + chunkName + "|" + data;
						try {
							outServerOne.println(messageToSecondaryServers);
							outServerOne.flush();
							secondarySocketOne.close();
							outServerTwo.println(messageToSecondaryServers);
							outServerTwo.flush();
							secondarySocketTwo.close();
							System.out.println("%%%%%%%%%%Successful secondary server commit" + messageToSecondaryServers);
						} catch (Exception ex) {
							System.out.println("ServerAppend Failed second phase. Failed to connect to the secondary server");	
							ex.printStackTrace();
							clientOutServer = new PrintWriter(mySocket.getOutputStream());		
							clientOutServer.println("ServerAppend Failed second phase. Server unavailable");
							System.out.println("^^^^^^^^^^^^ ServerAppend Failed second phase. Server unavailable");
							clientOutServer.flush();
							mySocket.close();
							return;
							//break;
						}

						File file =new File("ChunkFolder_"+ServerMain.id+"/"+chunkName);
						FileWriter fileWritter = null;
						if(file.exists()){
							//true = append file
							fileWritter = new FileWriter("ChunkFolder_"+ServerMain.id+"/"+chunkName,true);
						} else {
							fileWritter = new FileWriter("ChunkFolder_"+ServerMain.id+"/"+chunkName);
						}
						BufferedWriter bufferWritter = new BufferedWriter(fileWritter);
						bufferWritter.write(data);
						bufferWritter.close();
						ServerMain.chunksHavingMeAsMasterServer.put(chunkName, 1);
						clientOutServer = new PrintWriter(mySocket.getOutputStream());		
						clientOutServer.println("ACK");
						clientOutServer.flush();
						secondarySocketOne.close();
						secondarySocketTwo.close();
						System.out.println("&&&&&&&&&&&&&&&&    Creation successful");
					}
					//ServerAppend
				} else if(arr[0].equals("ServerAppend")) {
					//"ServerAppend|" + ServerMain.myDomain + "|" + chunkName + "|" + data;
					String serverName = arr[1];
					String chunkName = arr[2];
					String data = arr[3];
					System.out.println("1111111111 in ServerAppend " + request);
					PrintWriter outServer = new PrintWriter(mySocket.getOutputStream());
					BufferedReader inReader = new BufferedReader(new InputStreamReader(mySocket.getInputStream()));
					outServer.println("ACK");
					outServer.flush();
					System.out.println("2222222222 Ack from ss1");
					String messageFromPrimary = null;
					while((messageFromPrimary = inReader.readLine()) == null){
						try {
							Thread.sleep(100);
							System.out.println("333333333333 messageFromPrimary " + messageFromPrimary);
						} catch (InterruptedException e1) {
							System.out.println("Append failed.Error in secondary server"+ServerMain.mServerDomain);
						}        
					}
					if(messageFromPrimary.startsWith("ServerNoCommit")) {
						System.out.println(serverName + " No commit of the chunk.Append failed.Error in secondary server"+ServerMain.mServerDomain);
					} else if(messageFromPrimary.startsWith("ServerCommit")) {
						System.out.println("444444444444444 messageFromPrimary " + messageFromPrimary);
						File file =new File("ChunkFolder_"+ServerMain.id+"/"+chunkName);
						FileWriter fileWritter = null;
						if(file.exists()){
							//true = append file
							fileWritter = new FileWriter("ChunkFolder_"+ServerMain.id+"/"+chunkName,true);
						} else {
							fileWritter = new FileWriter("ChunkFolder_"+ServerMain.id+"/"+chunkName);
						}
						BufferedWriter bufferWritter = new BufferedWriter(fileWritter);
						bufferWritter.write(data);
						bufferWritter.close();
					}
				} else if(arr[0].equals("MServerCreate")) {
					//"MServerCreate|"+"*"+chunkName+"|"+MetaServer.convertToString(serverNames.toString()) + "|" + MetaServer.myDomain;
					System.out.println("11111111111 inside MServerCreate " + request);
					String chunkName = arr[1];
					String[] serversToFetchFrom = arr[2].split(",");
					if(chunkName.startsWith("*")){
						chunkName = arr[1].substring(1);
						ServerMain.chunksHavingMeAsMasterServer.put(chunkName, 1);
					}
					String readMessageToServer = "ServerRead|"+chunkName+"|"+ ServerMain.myDomain;
					Socket serverSend = new Socket(serversToFetchFrom[0],6000);
					PrintWriter outToSend = new PrintWriter(serverSend.getOutputStream());
					outToSend.println(readMessageToServer);
					outToSend.flush();
					System.out.println("222222222 readMessageToServer " + serversToFetchFrom[0] + readMessageToServer);
					BufferedReader inServer = new BufferedReader(new InputStreamReader(serverSend.getInputStream()));
					String replyFromPrimaryServer = null;
					while((replyFromPrimaryServer = inServer.readLine()) == null){
						try {
							Thread.sleep(100);
							System.out.println("333333333333333 replyFromPrimaryServer " + replyFromPrimaryServer);
						} catch (InterruptedException e1) {
							e1.printStackTrace();
						}        
					}
					serverSend.close();
					if(replyFromPrimaryServer.length() != 0) {
						FileWriter fileWritter = null;
						fileWritter = new FileWriter("ChunkFolder_"+ServerMain.id+"/"+chunkName);								
						BufferedWriter bufferWritter = new BufferedWriter(fileWritter);
						bufferWritter.write(replyFromPrimaryServer);
						bufferWritter.close();

						System.out.println(arr[2].split(",")[1] + " MServer fetch success!!!!!");	

						PrintWriter mServerOutToSend = new PrintWriter(mySocket.getOutputStream());
						mServerOutToSend.println("ACK");
						mServerOutToSend.flush();
						System.out.println("333333333333333 replyFromPrimaryServer " + replyFromPrimaryServer);
						mySocket.close();
						return;
					} else {
						Socket serverSendTwo = new Socket(serversToFetchFrom[1],6000);
						PrintWriter outToSendTwo = new PrintWriter(serverSendTwo.getOutputStream());
						outToSendTwo.println(readMessageToServer);
						outToSendTwo.flush();
						System.out.println("444444444444 MServerCreate serversToFetchFrom[1] " + serversToFetchFrom[1]);
						BufferedReader inServerTwo = new BufferedReader(new InputStreamReader(serverSendTwo.getInputStream()));
						String replyFromPrimaryServerTwo = null;
						while((replyFromPrimaryServerTwo = inServerTwo.readLine()) == null){
							try {
								Thread.sleep(100);
								System.out.println("55555555 replyFromPrimaryServerTwo " + replyFromPrimaryServerTwo);
							} catch (InterruptedException e1) {
								e1.printStackTrace();
							}        
						}
						serverSendTwo.close();
						if(replyFromPrimaryServerTwo.length() != 0) {									
							FileWriter fileWritter = null;
							fileWritter = new FileWriter("ChunkFolder_"+ServerMain.id+"/"+chunkName);									
							BufferedWriter bufferWritter = new BufferedWriter(fileWritter);
							bufferWritter.write(replyFromPrimaryServerTwo);
							bufferWritter.close();

							System.out.println(arr[2].split(",")[1] + " MServer fetch success!!!!!");	
							PrintWriter mServerOutToSend = new PrintWriter(mySocket.getOutputStream());
							mServerOutToSend.println("ACK");
							mServerOutToSend.flush();
						}
					}
				} else if(arr[0].equals("ServerRead")) {
					//"ServerRead|"+chunkName+"|"+ ServerMain.myDomain;
					System.out.println("1111111111 inside ServerRead "+request );
					FileInputStream fileInputStream=null;
					File file = new File("ChunkFolder_"+ServerMain.id+"/"+arr[1]);
					if(file.exists()){
						byte[] bFile = new byte[(int) file.length()];								
						//convert file into array of bytes
						fileInputStream = new FileInputStream(file);
						fileInputStream.read(bFile);
						fileInputStream.close();

						if(bFile != null){
							System.out.println("read from file : "+ new String(bFile,"UTF-8"));

							PrintWriter clientOut = new PrintWriter(mySocket.getOutputStream());
							clientOut.println(new String(bFile,"UTF-8"));
							clientOut.flush();
							
						}
					}
				}else if(arr[0].equals("Read")) {
					// Read|chunkname|start offset|end Offset|clientdomain
					System.out.println("1111111111 inside Read "+request );
					FileInputStream fileInputStream=null;
					File file = new File("ChunkFolder_"+ServerMain.id+"/"+arr[1]);
					if(file.exists()){
						System.out.println("22222 File exists " + request );
						byte[] bFile = new byte[(int) file.length()];
						byte[] readData = null;
						//convert file into array of bytes
						fileInputStream = new FileInputStream(file);
						fileInputStream.read(bFile);
						fileInputStream.close();

						if(Integer.parseInt(arr[3]) < bFile.length && Integer.parseInt(arr[2]) < bFile.length){
							readData = Arrays.copyOfRange(bFile, Integer.parseInt(arr[2]), Integer.parseInt(arr[3]));
						}else if(Integer.parseInt(arr[2]) < bFile.length){
							readData = Arrays.copyOfRange(bFile, Integer.parseInt(arr[2]), bFile.length);	
						}

						if(readData != null){
							System.out.println("read from file : "+ new String(readData,"UTF-8"));
							PrintWriter clientOut = new PrintWriter(mySocket.getOutputStream());
							clientOut.println(new String(readData,"UTF-8"));
							clientOut.flush();
							
						}else{
							PrintWriter clientOut = new PrintWriter(mySocket.getOutputStream());
							clientOut.println("ERROR: make sure offset is correct");
							System.out.println("ERROR: make sure offset is correct");
							clientOut.flush();
						}
					}
				} else if(arr[0].equals("MServerNullAppend")) {
					//MServerNullAppend|chunkName|s1,s2|numberOfNulls
					System.out.println("111111111111 in side MServerNullAppend");
					String chunkName = arr[1];			
					int numberOfNulls = Integer.parseInt(arr[3]);
					int ackCount = 0;
					String messageToSecondaryServers = "ServerNullAppend|" + ServerMain.myDomain + "|" + chunkName + "|" +numberOfNulls;
					PrintWriter printWriterMaster = null;
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
						System.out.println("22222 messageToSecondaryServers" + messageToSecondaryServers + " " + arr[2].split(",")[0]);
						String replyFromPrimaryServer = null;
						while((replyFromPrimaryServer = inServerOne.readLine()) == null){
							try {
								Thread.sleep(100);
								System.out.println("333333 messageToSecondaryServers" + messageToSecondaryServers);
							} catch (InterruptedException e1) {
								e1.printStackTrace();
								System.out.println("Failed to server null append first phase. Server unavailable " + secondarySocketOne);
								secondarySocketOne.close();
								mySocket.close();
								return;
							}        
						}

						if(replyFromPrimaryServer != null && replyFromPrimaryServer.startsWith("ACK")) {
							System.out.println(arr[2].split(",")[0] + " server null append first phase success!!!!!" + secondarySocketOne);	
							ackCount++;
						} else {
							printWriterMaster = new PrintWriter(mySocket.getOutputStream());		
							printWriterMaster.println("Failed to server null append first phase. Server unavailable" + secondarySocketOne);
							printWriterMaster.flush();
							mySocket.close();
							secondarySocketOne.close();
							System.out.println("444444444444444444 Failed to server null append first phase. Server unavailable" + secondarySocketOne);
							return;
						}
					} catch(Exception ex) {
						ex.printStackTrace();
						printWriterMaster = new PrintWriter(mySocket.getOutputStream());		
						printWriterMaster.println("Failed to server null append first phase. Server unavailable" + secondarySocketOne);
						System.out.println("55555555555555555 Failed to server null append first phase. Server unavailable" + secondarySocketOne);
						printWriterMaster.flush();
						mySocket.close();
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
						System.out.println("666666666666666666666666    messageToSecondaryServers  " + arr[2].split(",")[1]);
						String replyFromPrimaryServer = null;
						while((replyFromPrimaryServer = inServerTwo.readLine()) == null){
							try {
								Thread.sleep(100);
								System.out.println("7777777777777777    replyFromPrimaryServer  " + replyFromPrimaryServer);
							} catch (InterruptedException e1) {
								System.out.println("Failed to server append null first phase. Server unavailable " + secondarySocketTwo);
								messageToSecondaryServers = "ServerNullAppendNoCommit|" + ServerMain.myDomain + "|" + chunkName;
								outServerOne.println(messageToSecondaryServers);
								outServerOne.flush();
								System.out.println("8888888888888888888888888    replyFromPrimaryServer  " + replyFromPrimaryServer);
								e1.printStackTrace();
								printWriterMaster = new PrintWriter(mySocket.getOutputStream());		
								printWriterMaster.println("Failed to server append null first phase. Server unavailable");
								System.out.println("9999999999999999999    replyFromPrimaryServer  " + replyFromPrimaryServer);
								printWriterMaster.flush();
								mySocket.close();
								secondarySocketOne.close();
								secondarySocketTwo.close();
								return;
								//break;
							}        
						}
						if(replyFromPrimaryServer != null && replyFromPrimaryServer.startsWith("ACK")) {
							System.out.println(arr[2].split(",")[1] + " server append null first phase success!!!!!");
							ackCount++;
						} else {
							messageToSecondaryServers = "ServerNullAppendNoCommit|" + ServerMain.myDomain + "|" + chunkName;
							outServerOne.println(messageToSecondaryServers);
							outServerOne.flush();
							System.out.println("~~~~~~~~~ " + messageToSecondaryServers);
							printWriterMaster = new PrintWriter(mySocket.getOutputStream());		
							printWriterMaster.println("Failed to server append null first phase. Server unavailable " + ServerMain.myDomain);
							System.out.println("!!!!!!!!  Failed to server append null first phase. Server unavailable " + ServerMain.myDomain);
							printWriterMaster.flush();
							secondarySocketOne.close();
							secondarySocketTwo.close();
							return;
						}
					} catch(Exception ex) {
						ex.printStackTrace();
						messageToSecondaryServers = "ServerNullAppendNoCommit|" + ServerMain.myDomain + "|" + chunkName;
						outServerOne.println(messageToSecondaryServers);
						outServerOne.flush();
						System.out.println("@@@@@@@@@ messageToSecondaryServers" + messageToSecondaryServers);
						printWriterMaster = new PrintWriter(mySocket.getOutputStream());		
						printWriterMaster.println("Failed to server append null first phase. Server unavailable " + ServerMain.myDomain);
						System.out.println("######### Failed to server append null first phase. Server unavailable " + ServerMain.myDomain);
						printWriterMaster.flush();
						mySocket.close();
						secondarySocketOne.close();
						secondarySocketTwo.close();
						return;
					}

					if(ackCount == 2) {
						messageToSecondaryServers = "ServerNullCommit|" + ServerMain.myDomain + "|" + chunkName;
						try {
							outServerOne.println(messageToSecondaryServers);
							outServerOne.flush();
							secondarySocketOne.close();
							outServerTwo.println(messageToSecondaryServers);
							outServerTwo.flush();
							secondarySocketTwo.close();
							System.out.println("$$$$$$$$$$  " + messageToSecondaryServers);
						} catch (Exception ex) {
							System.out.println("server append null Failed second phase. Failed to connect to the secondary server");	
							ex.printStackTrace();
							printWriterMaster = new PrintWriter(mySocket.getOutputStream());		
							printWriterMaster.println("server append null Failed second phase. Server unavailable");
							System.out.println("%%%%%%%%%%% server append null Failed second phase. Server unavailable");
							printWriterMaster.flush();
							mySocket.close();
							return;
							//break;
						}

						File file =new File("ChunkFolder_"+ServerMain.id+"/"+chunkName);
						FileWriter fileWritter = null;
						if(file.exists()){
							//true = append file
							fileWritter = new FileWriter("ChunkFolder_"+ServerMain.id+"/"+chunkName,true);
						} else {
							fileWritter = new FileWriter("ChunkFolder_"+ServerMain.id+"/"+chunkName);
						}
						BufferedWriter bufferWritter = new BufferedWriter(fileWritter);
						byte[] nullArr = new byte[numberOfNulls];
						for(int i=0;i<nullArr.length;i++){
							nullArr[i] = 0;
						}
						bufferWritter.write(new String(nullArr, "UTF-8"));
						bufferWritter.close();
						printWriterMaster = new PrintWriter(mySocket.getOutputStream());		
						printWriterMaster.println("ACK");
						printWriterMaster.flush();
						secondarySocketOne.close();
						secondarySocketTwo.close();
						System.out.println("^^^^^^^^^^^^^^^^ MServerNullAppend Done");
					}
					//ServerAppend
				} else if(arr[0].equals("ServerNullAppend")) {
					//"ServerNullAppend|" + ServerMain.myDomain + "|" + chunkName + "|" +numberOfNulls;
					System.out.println("111111111111111 inside ServerNullAppend");
					String serverName = arr[1];
					String chunkName = arr[2];
					int numberOfNulls = Integer.parseInt(arr[3]);

					PrintWriter outServer = new PrintWriter(mySocket.getOutputStream());
					BufferedReader inReader = new BufferedReader(new InputStreamReader(mySocket.getInputStream()));
					outServer.println("ACK");
					outServer.flush();
					System.out.println("22222222222 ServerNullAppend" +  mySocket.toString());
					String messageFromPrimary = null;
					while((messageFromPrimary = inReader.readLine()) == null){
						try {
							Thread.sleep(100);
							System.out.println("33333333333 messageFromPrimary " +  messageFromPrimary);
						} catch (InterruptedException e1) {
							System.out.println("Null Append failed.Error in secondary server"+ServerMain.mServerDomain);
						}        
					}
					if(messageFromPrimary.startsWith("ServerNullAppendNoCommit")) {
						System.out.println(serverName + " No commit of the chunk. Null Append failed.Error in secondary server"+ServerMain.mServerDomain);
					} else if(messageFromPrimary.startsWith("ServerNullCommit")) {
						File file =new File("ChunkFolder_"+ServerMain.id+"/"+chunkName);
						FileWriter fileWritter = null;
						if(file.exists()){
							//true = append file
							fileWritter = new FileWriter("ChunkFolder_"+ServerMain.id+"/"+chunkName,true);
						} else {
							fileWritter = new FileWriter("ChunkFolder_"+ServerMain.id+"/"+chunkName);
						}
						BufferedWriter bufferWritter = new BufferedWriter(fileWritter);
						byte[] nullArr = new byte[numberOfNulls];
						for(int i=0;i<nullArr.length;i++){
							nullArr[i] = 0;
						}
						bufferWritter.write(new String(nullArr, "UTF-8"));
						bufferWritter.close();
						System.out.println("44444444444 ServerNullAppend successed");
					}
				}
				if(mySocket != null && !mySocket.isClosed())
					mySocket.close();
			}
		}catch(Exception e){
			e.printStackTrace();
		}
	}
}