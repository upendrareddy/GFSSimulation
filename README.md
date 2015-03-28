Client-Server Model
	In this project you are to emulate a file system and a set of clients accessing the files. You may require the knowledge of
thread and/or socket programming and its APIs of the language you choose. It can be assumed that processes (server/client)
are running on different machine (dcXX).

Description
	Design a distributed system with three file servers, two clients and a metadata server (M-server) to emulate a distributed
file system. Your program should be easily extensible for
any number of servers and clients. The file system you are required to emulate is one with one directory and a number of
text files in that directory. A file in this file system can be of any size. However, a file is logically divided into chunks, with
each chunk being at most 8192 bytes in size. Chunks of files in your filesystem are actually stored as Linux files on the
three servers. All the chunks of a given file need not be on the same server.
	In steady state, the M-server maintains the following metadata about files in your file system: file name, names of
Linux files that correspond to the chunks of the file, which server hosts which chunk, when was a chunk to server mapping
last updated.
	Initially, the M-server does not have the chunk name to server mapping, nor does it have the corresponding time of last
mapping update. Every 5 seconds, the servers send heartbeat messages to the M-server with the list of Linux files they
store. The M-server uses these heartbeat messages to populate/update the metadata.
	If the M-server does not receive a heartbeat message from a server for 15 seconds, the M-server assumes that the server
is down and none of its chunks is available.
	Clients wishing to create a file, read or append to a file in your file system send their request (with the name of the file)
to the M-server. If a new file is to be created, the M-server randomly asks one of the servers to create the first chunk of the
file, and adds an entry for that file in its directory. For read and append operations, based on the file name and the offset,
the M-server determines the chunk, and the offset within that chunk where the operation has to be performed, and sends
the information to the client. Then, the client directly contacts the corresponding server and performs the operations.
In effect, clients and servers communicate with each other to exchange data, while the clients and servers communicate
with the M-server to exchange metadata.
	You can assume that the maximize amount of data that can be appended at a time is 2048 bytes. If the current size of
the last chunk of the file, where the append operation is to be performed, is S such that 8192 ?? S < appended data size
then the rest of that chunk is padded with a null character, a new chunk is created and the append operation is performed
there.

Operations
1. Your code should be able to support creation of new files, reading of existing files, and appends to the end of existing
files.
2. If a server goes down, your M-server code should be able to detect its failure on missing three heartbeat messages
and mark the corresponding chunks as unavailable. While those chunks are available, any attempt to read or append
to them should return an error message.
3. When a failed server recovers, it should resume sending its heartbeat messages, and the M-server which should
repopulate its metadata to indicate the availability fo the recovered server’s chunks.
In this project you will extend your Project 2 code to implement these additional features:
4. Three replicas (at three different servers) are maintained for each chunk.
5. All replicas of a chunk are consistent to begin with.
6. If all replicas of a chunk are consistent before an append, then they must be consistent on completion of the append.
7. In order to ensure replica consistency, if two clients concurrently submit append operations for the same chunk, the
two appends should be applied in the same sequence at all the replicas of the chunk.
8. A read operation can be performed at any of the three replicas.
9. If a chunk server, X, crashes, thus reducing the number of replicas of the chunks hosted by it by one, for each chunk
stored on X, the M-server must identify a live chunk server, Y , and create one new copy of that chunk on Y . You
must not copy all chunks previously hosted by X to a single live chunk server, Y . Instead, the responsibility should
be distributed among a subset of live chunk servers. Make sure that a chunk server that already has a replica of a
chunk stored on X does not get a second replica of the same chunk.
10. When assigning a chunk to a chunk server (either when a new chunk is created or while handling the failure of a
chunk server), the M-server tries to achieve load balancing. For this purpose, each chunk server sends information
about how many chunks it is already hosting and how much free space it has. The M-server uses this information to
determine where a chunk is to be placed.
11. You must have at least seven chunk servers and five clients.