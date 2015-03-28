import java.io.File;
import java.io.IOException;
import java.io.PrintWriter;
import java.net.Socket;
import java.util.TreeMap;

public class SendHeartBeatMessage extends Thread {
	private int freeSpace;
	public TreeMap<String,String> getHeartBeatMessageToBeSent(){
		TreeMap<String,String> chunkInfo = new TreeMap<String,String>();
		File folder = new File("ChunkFolder_"+ServerMain.id);
		File[] listOfFiles = folder.listFiles();
		int filesSize = 0;
		for (int i = 0; i < listOfFiles.length; i++) {
			if (listOfFiles[i].isFile()) {
				if(ServerMain.chunksHavingMeAsMasterServer.containsKey(listOfFiles[i])) {
					System.out.println("Primary server for File " + listOfFiles[i].getName()+" time :"+System.currentTimeMillis());
					chunkInfo.put("*"+listOfFiles[i].getName(), listOfFiles[i].length()+"");
				} else {
					chunkInfo.put(listOfFiles[i].getName(), listOfFiles[i].length()+"");
				}
				filesSize += 8192;
			}
		}
		freeSpace = ServerMain.capacity - filesSize;
		return chunkInfo;
	}

	public void run(){
		try{
			while(true){
				System.out.println("starting send heartbeat thread!!!!!.............");
				TreeMap<String,String> myDirectoryInfo = new TreeMap<String,String>();
				myDirectoryInfo = getHeartBeatMessageToBeSent();
				String message ="HeartbeatMessage|"+ServerMain.myDomain+"|" + freeSpace ;
				if(myDirectoryInfo.size() != 0)	{			
					message = message+"|"+ ServerMain.convertToString(myDirectoryInfo.toString());
				}
				Socket mServer = new Socket(ServerMain.mServerDomain,6000);
				PrintWriter outputStream = new PrintWriter(mServer.getOutputStream());
				outputStream.println(message);
				outputStream.flush();
				System.out.println("sent heartbeat.."+message);
				mServer.close();
				Thread.sleep(4500);
			}
		}catch(InterruptedException | IOException e){
			e.printStackTrace();
		}
	}
}
