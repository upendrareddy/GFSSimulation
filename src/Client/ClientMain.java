import java.io.BufferedReader;
import java.io.FileReader;

public class ClientMain {

	static String myDomain;
	static String domainOfMServer;

	public static void setDomains(String id){
		BufferedReader br = null;
		try {
			String filename = "/home/004/d/dx/dxr131330/workspace/AOSProject3/src/config.txt";
			String theCurrentLine = null;
			br = new BufferedReader(new FileReader(filename));
			while((theCurrentLine = br.readLine())!= null) {
				String msgRead[] = theCurrentLine.split(" ");
				System.out.println(theCurrentLine+" " + msgRead[0]);
				if(msgRead[0].equals(id)){
					myDomain = msgRead[1];
				}else if(msgRead[0].equals("mserver")){
					domainOfMServer = msgRead[1];
				}
			}
			System.out.println("myDomain :" +myDomain+" mserver :"+domainOfMServer);
		}catch(Exception e)	{
			e.printStackTrace();
		}
	}
	
	public static void setup(String clientId){
		setDomains(clientId);
	}
	
	public static void generateOutput(String filename){
		BufferedReader br = null;
		ProcessOperations r = new ProcessOperations();
		try {
			String theCurrentLine = null;
			br = new BufferedReader(new FileReader(filename));
			while((theCurrentLine = br.readLine())!= null) {
				String msgRead[] = theCurrentLine.split("\\|");
				if(msgRead[0].equals("r")){
					System.out.println("!!!!!!!!!!!!!!!!!read operation|"+theCurrentLine);
					r.read(theCurrentLine);
				}else if(msgRead[0].equals("w")){
					System.out.println("!!!!!!!!!!!!!!!!!write opetation|"+msgRead[0]+"|"+msgRead[1]);
					r.create(theCurrentLine);
				}else if(msgRead[0].equals("a")){
					System.out.println("!!!!!!!!!!!append operation|"+msgRead[0]+"|"+msgRead[1]);
					r.append(theCurrentLine);
				}
			}
		}
		catch(Exception e) {
			e.printStackTrace();
		}
	}
	
	public static void main(String[] args){
		if(args[0] != null){
			setup(args[0]);
		}else{
			System.out.println("Error. enter client id.");
		}
		
		if(args[1] != null){
			generateOutput(args[1]);
		}else {
			System.out.println("Error.enter filename");
		}
	}
}