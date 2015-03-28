import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.FileWriter;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.PrintWriter;
import java.net.ServerSocket;
import java.net.Socket;
import java.util.Arrays;
import java.util.HashMap;

public class ProcessOperations {

	public void writeToFile(String message) {
		try {
			PrintWriter out = new PrintWriter(new BufferedWriter(new FileWriter("client.txt", true)));
			out.println(message);
			out.close();
		} catch (IOException e) {
			System.out.println("client : write to file failed");
		}
	}

	public void read(String operation) throws IOException{
		String[] arr = operation.split("\\|");
		/**
		 *r|filename|offset|no of bytes to read
		 */
		if(arr.length == 4){
			String filename = arr[1].split("\\.")[0];
			int offset = Integer.parseInt(arr[2]);
			int noOfBytesToRead = Integer.parseInt(arr[3]);
			//ClientRead|clientname|filename
			String toSendMServer = "ClientRead|"+ClientMain.myDomain+"|"+filename;
			Socket mserver = new Socket(ClientMain.domainOfMServer,6000);
			PrintWriter out = new PrintWriter(mserver.getOutputStream());
			out.println(toSendMServer);
			out.flush();
			BufferedReader in = new BufferedReader(new InputStreamReader(mserver.getInputStream()));
			String message = null;

			while((message = in.readLine()) == null){
				try {
					Thread.sleep(100);
				} catch (InterruptedException e1) {
					e1.printStackTrace();
				}	
			}
			mserver.close();
			System.out.println("&&&&&&&&&&&&&&message received from mserver : "+message);
			if(message != null){
				if(message.startsWith("Error")){
					System.out.println("ERROR : file not available or server not available.Try again later.");
					return;
				}
				/**
				 * key1=val1,val2,val3|key2=val1,val2,val3...
				 *  <key=chunkname,<value = servername>>
				 */
				HashMap<String,String[]> serverChunkMapping = new HashMap<String,String[]>();
				message = message.split("--")[1];
				for(String s:message.split("\\$")){
					String[] keyValues = s.split("=");
					String key = keyValues[0];
					String values = keyValues[1];
					String[] servers = values.split(",");
					for(int i = 0; i < servers.length; i++) {
						System.out.println("%%%%%%%%%" +servers[i]);
					}
					if(!key.isEmpty() && servers != null && servers.length != 0) {
						System.out.println("$$$$$$$$$ " + key);
						serverChunkMapping.put(key, servers);
					}
				}

				int chunkNumber = (int)Math.ceil((double)offset/8191);

				int lastChunkNumber = (int)Math.ceil((double)(offset+noOfBytesToRead)/8191);
				int temp = chunkNumber;
				System.out.println("starting chunk :"+temp);
				System.out.println("last chunk number :"+lastChunkNumber);
				writeToFile("starting chunk :"+temp+" "+"last chunk number :"+lastChunkNumber);
				while(temp <= lastChunkNumber){
					int offsetWithinfile ;
					if(temp == chunkNumber)
						offsetWithinfile = offset - (chunkNumber-1)*8192;
					else
						offsetWithinfile = 0;

					int endingPoint;

					if(lastChunkNumber == temp){
						endingPoint = (noOfBytesToRead + offset) - (lastChunkNumber-1)*8192;
					}else{
						endingPoint = 8192;
					}

					System.out.println("offset within file:"+offsetWithinfile);
					System.out.println("ending point of this chunk :"+endingPoint);
					writeToFile("offset within file:" + offsetWithinfile + " "+"ending point of this chunk :"+endingPoint);
					String chunkName = filename+"_"+temp;

					String[] serverNamesToSend = null;
					if(serverChunkMapping.containsKey(chunkName)){
						serverNamesToSend = serverChunkMapping.get(chunkName);
						System.out.println(serverChunkMapping.get(chunkName));
					}
					else{
						System.out.println("ERROR: server/chunk unavailable");
						temp++;
						continue;
					}
					int counter = 0;

					while(counter < serverNamesToSend.length){
						try{
							String sendRequestToServer = "Read|"+chunkName+"|"+offsetWithinfile+"|"+endingPoint+"|"+ClientMain.myDomain;
							System.out.println(sendRequestToServer+"..."+serverNamesToSend[counter]);
							Socket serverSend = new Socket(serverNamesToSend[counter],6000);
							PrintWriter outToSend = new PrintWriter(serverSend.getOutputStream());
							outToSend.println(sendRequestToServer);
							outToSend.flush();
							BufferedReader inServer = new BufferedReader(new InputStreamReader(serverSend.getInputStream()));
							String readData = null;
							while((readData = inServer.readLine()) == null){
								try {
									Thread.sleep(100);
								} catch (InterruptedException e1) {
									e1.printStackTrace();
								}	
							}
							serverSend.close();					
							System.out.println("Data from "+offsetWithinfile+" to "+endingPoint+" :");
							System.out.println(readData);
							break;
						}
						catch(Exception e){
							counter++;
							System.out.println("failed connecting to server for read operation.time : "+System.currentTimeMillis());
							writeToFile("failed connecting to server for read operation.time : "+System.currentTimeMillis());
							e.printStackTrace();
						}
					}
					temp++;
				}
			}
		}
	}

	public void append(String operation){
		String[] arr = operation.split("\\|");
		/**
		 * a|filename|data
		 */
		if(arr[0].equals("a")){
			byte[] data = arr[2].getBytes();
			String filename = arr[1].split("\\.")[0];
			//ClientAppend|clientname|filename|data.length
			String toSendMServer = "ClientAppend|"+ClientMain.myDomain+"|"+filename+"|"+data.length;
			System.out.println("message to mserver :" + toSendMServer);
			try{
				Socket mserver = new Socket(ClientMain.domainOfMServer,6000);
				PrintWriter out = new PrintWriter(mserver.getOutputStream());
				out.println(toSendMServer);
				out.flush();

				BufferedReader in = new BufferedReader(new InputStreamReader(mserver.getInputStream()));
				String message = null;
				while((message = in.readLine())== null){
					Thread.sleep(100);
				}

				mserver.close();
				System.out.println("message from mserver :"+message);
				// Append--servername1,*servername2,servername3|chunkname
				if(message != null){
					String msgArr[] = message.split("--");

					if(msgArr[0].equals("Append")){
						String[] appendParameters = msgArr[1].split("\\|");
						String[] serverArray = appendParameters[0].split(",");
						String chunkName = appendParameters[1];
						/**
						 * Append|chunkname|data
						 */
						int primaryServerIndex = findPrimaryServerIndex(serverArray);
						System.out.println("index :"+primaryServerIndex);
						if(primaryServerIndex != -1) {
							int attempt = 0;
							while(attempt < 2){
								try{
									StringBuffer secondaryServers = new StringBuffer();
									for(int i = 0; i < serverArray.length; i++) {
										if(i != primaryServerIndex) {
											secondaryServers.append(serverArray[i] +",");
										}
									}
									//server : ClientAppend|clientname|s1,s2|chunkname|data
									secondaryServers.deleteCharAt(secondaryServers.length() - 1);
									String sendChunkToServer = "ClientAppend|"+ClientMain.myDomain + "|" + secondaryServers.toString() + "|"
											+ chunkName + "|" + arr[2];
									System.out.println("message to sec :::::::"+sendChunkToServer+".........."+serverArray[primaryServerIndex].substring(1));
									Socket chunkServer = new Socket(serverArray[primaryServerIndex].substring(1),6000);
									PrintWriter outServer = new PrintWriter(chunkServer.getOutputStream());
									outServer.println(sendChunkToServer);
									outServer.flush();

									BufferedReader inServer = new BufferedReader(new InputStreamReader(chunkServer.getInputStream()));
									String replyFromPrimaryServer = null;
									while((replyFromPrimaryServer = inServer.readLine()) == null){
										try {
											Thread.sleep(100);
										} catch (InterruptedException e1) {
											e1.printStackTrace();
										}        
									}
									chunkServer.close();
									
									if(replyFromPrimaryServer != null && !replyFromPrimaryServer.isEmpty() 
											&& replyFromPrimaryServer.startsWith("ACK")) {
										System.out.println("Append success!!!!!");										
									} else {
										System.out.println("Append Failure " + replyFromPrimaryServer);
									}
									break;
								}catch(Exception e){
									System.out.println("Append Failed. Failto connect to the primary server");
									attempt++;
									e.printStackTrace();
								}
							}
						}
					}else if(msgArr[0].equals("Append error")){
						String[] serverName = msgArr[1].split(" ");
						System.out.println("ERROR : The last chunk server "+serverName[1]+" unavailable");
					}
				}
			}catch(Exception e){
				e.printStackTrace();
				System.out.println("Error in append");
			}
		}
	}

	private int findPrimaryServerIndex(String[] serverArray) {
		for(int i = 0; i < serverArray.length; i++) {
			if(serverArray[i].startsWith("*")) {
				return i;
			}
		}
		return -1;
	}

	public void create(String operation){
		String[] arr = operation.split("\\|");
		if(arr[0].equals("w")) {
			byte[] data = arr[2].getBytes();
			/* w|file1.txt|Morbi sfjds */
			System.out.println("inside create");
			String filename = arr[1].split("\\.")[0];
			//ClientCreate|clientname|filename|data.length
			String toSend = "ClientCreate|"+ClientMain.myDomain+"|"+filename+"|"+data.length;
			System.out.println("string to mserver "+toSend);
			try{
				Socket mserver = new Socket(ClientMain.domainOfMServer,6000);
				PrintWriter out = new PrintWriter(mserver.getOutputStream());
				out.println(toSend);
				out.flush();
				BufferedReader in = new BufferedReader(new InputStreamReader(mserver.getInputStream()));
				String message = null;
				while((message = in.readLine())== null){
					Thread.sleep(100);
				}
				mserver.close();
				//Create--chunkName1=server1,*server2,server3|chunkName2=server3,*server4,server5
				if(message != null){
					System.out.println("message received from mserver "+message);
					if(message.startsWith("Error:")){
						System.out.println("Error: file already exists!!Try another name.");
						return;
					}
					String messageArr[] = message.split("--");

					if(messageArr.length > 0 && messageArr[1].startsWith("error")){
						System.out.println("ERROR: all servers are down. Try again later.");
					}
					else if(messageArr.length > 0 && messageArr[1] != null){
						String[] chunkAndServers = messageArr[1].split("\\|");
						/*
						 * 1.split data into chunks
						 * 2.send each chunk to server resp.
						 */
						for(int i=0;i<chunkAndServers.length;i++){
							byte[] chunk;
							if(i==chunkAndServers.length-1){
								chunk = Arrays.copyOfRange(data, i*8192, data.length);
							}else{
								chunk = Arrays.copyOfRange(data, i*8192, (i+1)*8192-1);	
							}
							
							String chunkName = chunkAndServers[i].split("=")[0]; 
							String serverString = chunkAndServers[i].split("=")[1];
							String[] serverArray = serverString.split(",");
							int primaryServerIndex = findPrimaryServerIndex(serverArray);		
							System.out.println(chunkAndServers.toString());
							int counter = 0;
							System.out.println("length:"+chunk.length);
							while(counter < 2){
								try{
									
									StringBuffer secondaryServers = new StringBuffer();
									for(int j = 0; j < serverArray.length; j++) {
										if(j != primaryServerIndex) {
											secondaryServers.append(serverArray[j] +",");
										}
									}
									//server : ClientCreate|clientname|s1,s2|chunkname|data
									secondaryServers.deleteCharAt(secondaryServers.length() - 1);
									String sendChunkToServer = "ClientCreate|"+ClientMain.myDomain + "|" + secondaryServers.toString() + "|"
											+ chunkName + "|" + arr[2];

									Socket chunkServer = new Socket(serverArray[primaryServerIndex].substring(1),6000);
									PrintWriter outServer = new PrintWriter(chunkServer.getOutputStream());
									outServer.println(sendChunkToServer);
									outServer.flush();
									BufferedReader inServer = new BufferedReader(new InputStreamReader(chunkServer.getInputStream()));
									String replyFromPrimaryServer = null;
									while((replyFromPrimaryServer = inServer.readLine()) == null){
										try {
											Thread.sleep(100);
										} catch (InterruptedException e1) {
											e1.printStackTrace();
										}        
									}
									chunkServer.close();
									if(replyFromPrimaryServer != null && replyFromPrimaryServer.startsWith("ACK")) {
										System.out.println("Create success!!!!!");										
									} else {
										System.out.println("Create Failed." + replyFromPrimaryServer);
									}									
									break;
								}catch(Exception e){
									counter++;
									System.out.println("Error during creation."+"Creation of chunk :"+chunkName+" failed"+
											"Attempt "+counter+" failed.");
									e.printStackTrace();
								}
							}
						}
					}
				}
			}catch(Exception e){
				e.printStackTrace();
				System.out.println("Error in creation");
			}
		}
	}
}
