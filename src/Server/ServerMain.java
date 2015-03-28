import java.io.BufferedReader;
import java.io.File;
import java.io.FileReader;
import java.util.HashMap;
import java.util.Map;

public class ServerMain {
	public static String myDomain;
	public static String mServerDomain;
	public static String id;
	public static int capacity;
	public static Map<String, Integer> chunksHavingMeAsMasterServer = new HashMap<String, Integer>();
	//public static Queue<QueueMessage> processingQueue = new LinkedList<QueueMessage>();
	
	public static String convertToString(String toBeConverted){
		String temp = toBeConverted.substring(1,toBeConverted.length()-1);
		String[] tempArr = temp.split(",");
		temp = "";
		for(String s:tempArr){
			temp += s.trim()+",";
			System.out.println(temp);
		}
		return temp;
	}
	
	public static void setDomains(String serverId){
		BufferedReader br = null;
		try
		{
			String filename = "/home/004/d/dx/dxr131330/workspace/AOSProject3/src/config.txt";
			String theCurrentLine = null;
			br = new BufferedReader(new FileReader(filename));
			while((theCurrentLine = br.readLine())!= null)
			{
				String msgRead[] = theCurrentLine.split(" ");
				System.out.println(theCurrentLine+" "+msgRead[0]);
				if(msgRead[0].equals(serverId)){
					myDomain = msgRead[1];
				}else if(msgRead[0].equals("mserver")){
					mServerDomain = msgRead[1];
				}
			}
			System.out.println("mydomain : "+myDomain+" mserver : "+mServerDomain);
		}
		catch(Exception e)
		{
			e.printStackTrace();
		}
	}
	
	public static void setup(String serverId){
		//create a directory if it does not exist
		File file = new File("ChunkFolder_"+serverId);
		if (!file.exists()) {
			if (file.mkdir()) {
				System.out.println("Directory is created!");
			} else {
				System.out.println("Failed to create directory!");
			}
		}
		id = serverId;
		setDomains(serverId);
	}
	
	public static void main(String args[]){
		if(args[0] != null && args[1] != null){
			ServerMain.setup(args[0]);
			capacity = Integer.parseInt(args[1]);
			new ServerListener();
			SendHeartBeatMessage heartbeatMsg = new SendHeartBeatMessage();
			heartbeatMsg.start();
		}
	}
}