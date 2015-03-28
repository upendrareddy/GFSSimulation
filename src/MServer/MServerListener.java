import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.net.ServerSocket;
import java.net.Socket;

public class MServerListener extends Thread{

	ServerSocket serverSocket;
	MServerCommonStuff sharedObj = null;
	
	public MServerListener(MServerCommonStuff sharedObj){
		this.sharedObj = sharedObj;
		try{
			serverSocket = new ServerSocket(6000);
		}catch(IOException e){
			System.out.println("mserver listner cound not be started");
			e.printStackTrace();
		}
		start();
	}

	public void run(){
		while(true){
			try{
				//System.out.println("Listener thread...");
				Socket server = serverSocket.accept();
				//System.out.println("Just connected to "+ server.getRemoteSocketAddress());
				BufferedReader in = new BufferedReader(new InputStreamReader(server.getInputStream()));
				String message = in.readLine();
				System.out.println("message received :"+message);
				if(message != null){
					String messageArr[] = message.split("\\|");

					if( messageArr[0].equals("HeartbeatMessage")){
						new HeartBeatListner(message,sharedObj);
					//	System.out.println("spawning heartbeat thread..");
						in.close();
						server.close();
					}else if(messageArr[0].equals("ClientCreate")){
						new CreateChunk(message,server,sharedObj);
						System.out.println("spawning client thread :"+message);
					}else if(messageArr[0].equals("ClientAppend")){
						new ProcessAppendsAndReads(server, message,sharedObj);
					}else if(messageArr[0].equals("ClientRead")){
						new ProcessAppendsAndReads(server, message,sharedObj);
					}
				}
				Thread.sleep(100);
			}
			catch(IOException | InterruptedException e){
				System.out.println("Error in listener of M-Server");
				e.printStackTrace();
			}
		}
	}
}