import java.util.concurrent.ConcurrentHashMap;

public class MServerCommonStuff {

	//servers and the number of files they store. This information is used for load balancing
	ConcurrentHashMap<String, ServerProperties> serverInfoMap = new ConcurrentHashMap<String, ServerProperties>();

	//number of chunks each file has. Easy way to find out number of last chunk number.
	ConcurrentHashMap<String, Integer> fileAndNumberOfChunksMap = new ConcurrentHashMap<String, Integer>(); 

	//meta data about each file and server
	ConcurrentHashMap<String, ChunkInfo> chunkInfo = new ConcurrentHashMap<String, ChunkInfo>();
}
