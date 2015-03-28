import java.io.BufferedReader;
import java.io.FileReader;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Iterator;
import java.util.Map;
import java.util.Map.Entry;
import java.util.TreeMap;
import java.util.concurrent.PriorityBlockingQueue;

public class MetaServer {
	//all servers run on the same port 6000

	public static String myDomain;
	
	public static TreeMap<String, ArrayList<String>> getLoadBalancedServers(int startIndex, int numberOfChunks, String filename
			,MServerCommonStuff obj){
		TreeMap<String, ArrayList<String>> result = new TreeMap<String, ArrayList<String>>();
		PriorityBlockingQueue<ServerAndFreeSpace> heap = new PriorityBlockingQueue<ServerAndFreeSpace>();
		
		for(Iterator<Map.Entry<String, ServerProperties>> entry = obj.serverInfoMap.entrySet().iterator(); entry.hasNext();){
			Map.Entry<String, ServerProperties> current = entry.next();
			heap.add(new ServerAndFreeSpace(current.getKey(), current.getValue().getFreeSpace()));			
		}

	for(int i = startIndex; i < numberOfChunks+startIndex; i++) {
			ServerAndFreeSpace topOne = heap.poll();
			ServerAndFreeSpace topTwo = heap.poll();
			ServerAndFreeSpace topThree = heap.poll();
			System.out.println(topOne);
			System.out.println(topTwo);
			System.out.println(topThree);
			result.put(filename +"_" + (i + 1), new ArrayList<String>(Arrays.asList(topOne.getServerName(), topTwo.getServerName()
					, topThree.getServerName())));
			topOne.setFreeSpace(topOne.getFreeSpace()-8192); //May be a bug as we are subtracting 8192 by default every time. 
			topTwo.setFreeSpace(topTwo.getFreeSpace()-8192); //What about the last chunk case
			topThree.setFreeSpace(topThree.getFreeSpace()-8192);
			heap.add(topOne);
			heap.add(topTwo);
			heap.add(topThree);
		}

		return result;
	}
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
	public static void setDomains(){
		BufferedReader br = null;
		try{
			String filename = "/home/004/d/dx/dxr131330/workspace/AOSProject3/src/config.txt";
			String theCurrentLine = null;
			br = new BufferedReader(new FileReader(filename));
			while((theCurrentLine = br.readLine())!= null)
			{
				String msgRead[] = theCurrentLine.split(" ");
				System.out.println(theCurrentLine+" "+msgRead[0]);
				
				if(msgRead[0].equals("mserver")){
					myDomain = msgRead[1];
				}
			}
		}
		catch(Exception e)
		{
			e.printStackTrace();
		}
	}
	public static String convertToStringFromMap(TreeMap<String,ArrayList<String>> serverSizeMap) {
		StringBuffer stringBuffer = new StringBuffer();
		for(Entry<String, ArrayList<String>> entry : serverSizeMap.entrySet()) {
			stringBuffer.append(entry.getKey() + "=");
			for(int i = 0; i < entry.getValue().size(); i++) {
				stringBuffer.append(entry.getValue().get(i));
				if(i != entry.getValue().size()-1) {
					stringBuffer.append(",");
				}
			}
			stringBuffer.append("$");
		}
		return stringBuffer.toString();
	}
	
	public static void main(String[] args){
		System.out.println("in here");
		setDomains();
		MServerCommonStuff sharedObj = new MServerCommonStuff();
		new MServerListener(sharedObj);
		new CheckServerAliveThread(sharedObj);
	}
}