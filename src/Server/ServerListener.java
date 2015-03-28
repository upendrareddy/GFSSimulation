import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.net.ServerSocket;
import java.net.Socket;

public class ServerListener extends Thread{
	ServerSocket serverSocket;

	public ServerListener(){
		try{
			serverSocket = new ServerSocket(6000);
		}
		catch(IOException e){
			System.out.println("Unable to open socket.Listener thread failed!!");
			e.printStackTrace();
		}
		start();
	}

	public void run(){
		while(true){
			try{
				Socket server = serverSocket.accept();
				BufferedReader in = new BufferedReader(new InputStreamReader(server.getInputStream()));
				String message = in.readLine();
				if(message != null){
					System.out.println("message !!!!!!!!!"+message);
					if(message.startsWith("ClientCreate") || message.startsWith("ServerCreate")){
						new CreateRequestHandler(message, server);
					} else {
						new RequestHandler(message,server);
					}					
				}
				Thread.sleep(1000);
			}
			catch(IOException | InterruptedException e){
				System.out.println("Error in listener of Server");
				e.printStackTrace();
			}
		}
	}
}