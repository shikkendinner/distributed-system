package app_kvServer;

import common.messages.KVMessage;
import common.messages.KVAdminMessage;
import logger.LogSetup;
import org.apache.log4j.Level;
import org.apache.log4j.Logger;

import java.io.*;
import java.net.*;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;

import java.math.BigInteger;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;

public class KVServer implements Runnable {

	public static final String FIFO = "fifo";
	public static final String LRU = "lru";
	public static final String LFU = "lfu";
	public static final String DELETE = "delete";
	public static final String GET = "get";
	public static final String PUT = "put";
	public static final String SUBSCRIBE = "subscribe";
	public static final String UNSUBSCRIBE = "unsubscribe";

	public static final String INIT = "initkvserver";
	public static final String START = "start";
	public static final String STOP = "stop";
	public static final String SHUTDOWN = "shutdown";
	public static final String LOCKWRITE = "lockwrite";
	public static final String UNLOCKWRITE = "unlockwrite";
	public static final String MOVEDATA = "movedata";
	public static final String UPDATE = "update";
	public static final String DELETEPAIRS = "deletepairs";
	public static final String ADDKV = "addkvpairs";
	public static final String REPLICATE = "replicate";
	public static final String FIRSTREPLICADEAD = "firstreplicadead";
	public static final String HEARTBEAT = "heartbeat";
	public static final String FAIL = "fail";

	public static final int SERVER_STOPPED = 0;
	public static final int SERVER_READY = 1;

	public static final int BAN_WRITE = 0;
	public static final int ALLOW_WRITE = 1;

	//locks for requesting the following arrays
	private final Object lock = new Object();
	private final Object writeLock = new Object();
	private List<Integer> clientsInRequest = new ArrayList<Integer>();
	private List<Integer> writeRequests = new ArrayList<Integer>();

	//lock for metadata
	private final Object metaLock = new Object();
	private List<String> metadata = new ArrayList<String>();
	
	//lock for replica data files 
	//first indicates the files to access if the server is the first replica, second if it is second replica
	private final Object replicaLockFirst = new Object();
	private final Object replicaLockSecond = new Object();

	//lock for subscription file
	private final Object subscriptLock = new Object();
	
	private static Logger logger;

	private String ip;
	private int port;
	private String ecsAddr;
	private int ecsPort;
	
	private ServerSocket serverSocket;
	private boolean running;
	public int state;
	public int banWrite;

	private String serverHashStart;
	private String serverHashEnd;
	private String firstReplica;
	private String secondReplica;

	private storageServer mStorage = null;

	/* Start KV Server at given port
	 * @param port given port for storage server to operate
	 * @param cacheSize specifies how many key-value pairs the server is allowed
	 *           to keep in-memory
	 * @param strategy specifies the cache replacement strategy in case the cache
	 *           is full and there is a GET- or PUT-request on a key that is
	 *           currently not contained in the cache. Options are "FIFO", "LRU",
	 *           and "LFU".
	 */
	public KVServer(int port, String ecsAddr, int ecsPort) {
		this.port = port;
		this.ecsAddr = ecsAddr;
		this.ecsPort = ecsPort;
		
		state = SERVER_STOPPED;
		banWrite = ALLOW_WRITE;
		
		//set up logger
		try {
			new LogSetup("./logs/server/server.log", Level.ERROR);
			logger = Logger.getRootLogger();
		} catch (IOException e) {
			System.out.println("Error! Unable to initialize logger!");
			System.exit(1);
		}
	}

	/**
	 * Initializes and starts the server.
	 * Loops until the the server should be closed.
	 */
	public void run() {
		running = initializeServer();
		
		//this connection acts as a ping, to notify the ECS that the server is ready to listen to connections
		try {
			if(ecsAddr != null && ecsPort > 0){
				Socket ecsToServ = new Socket(ecsAddr.trim(), ecsPort);
				logger.info("CONNECTED TO ECS");
				ecsToServ.close();
			}
		} catch (UnknownHostException e1) {
			logger.error(e1.getMessage());
		} catch (IOException e1) {
			logger.error(e1.getMessage());
		}

		if (serverSocket != null) {
			while (isRunning()) {
				try {
					Socket client = serverSocket.accept();
					ClientConnectionKVServer connection = new ClientConnectionKVServer(client);
					connection.setKVServerListener(this);
					new Thread(connection).start();

					logger.info("Connected to "
							+ client.getInetAddress().getHostName()
							+ " on port " + client.getPort());
				} catch (IOException e) {
					logger.error("Error! " +
							"Unable to establish connection. \n", e);
				}
			}
		}
		logger.info("Server Shutdown");
	}

	public boolean isRunning() {
		return this.running;
	}

	//****************************CLIENT REQUEST METHODS****************************//

	/**
	 * Inserts a key-value pair into the KVServer.
	 *
	 * @param key   the key that identifies the given value.
	 * @param value the value that is indexed by the given key.
	 * @return a message that confirms the insertion of the tuple or an error.
	 * @throws Exception if put command cannot be executed
	 *                   (e.g. not connected to any KV server).
	 */

	public KVMessage put(String key, String value) throws Exception {
		KVMessage kvm;

		if (checkIfInRange(key, serverHashStart, serverHashEnd)) {
			logger.info("Putting  " + " " + "key: " + key + " " + "value: " + value);

			//add this request to the number of clients requesting list
			synchronized (lock) {
				clientsInRequest.add(0);
			}
			synchronized (writeLock) {
				writeRequests.add(0);
			}

			kvm = mStorage.put(key.trim(), value, "./data/storage"+port+".txt", "./data/temp"+port+".txt");

			//once done remove client's request from request list
			synchronized (lock) {
				clientsInRequest.remove(0);
			}
			synchronized (writeLock) {
				writeRequests.remove(0);
			}
			
			if(kvm.getStatus() == KVMessage.StatusType.DELETE_SUCCESS || 
					kvm.getStatus() == KVMessage.StatusType.PUT_SUCCESS ||
					kvm.getStatus() == KVMessage.StatusType.PUT_UPDATE){
				try{
					//notify subscribers
					if(kvm.getStatus() != KVMessage.StatusType.PUT_SUCCESS){
						tellSubscribers(key, value, "./data/subscriptions"+port+".txt");
					}
					
					//send write updates to replicas
					String valClean = (value == null) ? "null" : value;
					
					//first replica
					String[] ipAndPort = firstReplica.split(" ");
					String result = sendKvReplicaData(ipAndPort[0], Integer.parseInt(ipAndPort[1]), "server putreplica " + 1 + " " + key.trim() + " " + valClean);
					if(! result.equals("PUT_REPLICATION_COMPLETE")){
						throw new IOException("Replication of put to first replica failed!");
					}
					
					logger.info("Put replicated into first replica");
					
					ipAndPort = secondReplica.split(" ");
					result = sendKvReplicaData(ipAndPort[0], Integer.parseInt(ipAndPort[1]), "server putreplica " + 2 + " " + key.trim() + " " + valClean);
					if(! result.equals("PUT_REPLICATION_COMPLETE")){
						throw new IOException("Replication of put to second replica failed!");
					}
					
					logger.info("Put replicated into second replica");
				} catch(Exception e){
					logger.error(e.getMessage());
				}
			}	
		} else {
			//need to send updated metadata to client
			synchronized(metaLock){
				String bigData = metadata.get(0).trim();
				boolean skipFirst = false;

				for (String server : metadata) {
					if (skipFirst) {
						bigData = bigData + " " + server.trim();
					} else {
						skipFirst = true;
					}
				}
				//logger.error(bigData);
				kvm = new KVMessageStorage(bigData, value, KVMessage.StatusType.SERVER_NOT_RESPONSIBLE);
			}
		}

		return kvm;
	}
	
	private String sendKvReplicaData(String ip, int port, String command){
		//open a socket to the first replica, and send a putreplica command with the kv data
		String reply = "";
		Socket servToServ;
		try {
			servToServ = new Socket(ip, port);
			
			//get input/output stream
			PrintStream out = new PrintStream(servToServ.getOutputStream(), true);
			BufferedReader in = new BufferedReader(new InputStreamReader(servToServ.getInputStream()));

			//send the command + data to the server
			logger.info("Command to be sent: " + command);
			
			out.println(command);

			reply = in.readLine();
			
			servToServ.close();
		} catch (Exception e) {
			logger.error(e.getMessage());
		} 
		
		return reply;
	}
	
	public String putReplica(int indicator, String key, String value){
		String result = "PUT_REPLICATION_FAILED";
		
		String filePath = ((indicator == 1) ? "./data/storage"+port+"FR.txt" : "./data/storage"+port+"SR.txt");
		String tempPath = ((indicator == 1) ? "./data/temp"+port+"FR.txt" : "./data/temp"+port+"SR.txt");
		Object replicaLock = ((indicator == 1) ? replicaLockFirst : replicaLockSecond);
		
		synchronized(replicaLock){
			if(value.equals("null")){
				//a delete request!
				try {
					KVMessage kvm = mStorage.put(key, null, filePath, tempPath);
					if(kvm.getStatus() != KVMessage.StatusType.DELETE_SUCCESS){
						throw new Exception("DELETE_NOT_SUCCESSFUL");
					}
					result = "PUT_REPLICATION_COMPLETE";
				} catch (Exception e) {
					logger.error(e.getMessage());
				}
			} else {
				//a put update or a simple put
				try {
					KVMessage kvm = mStorage.put(key, value, filePath, tempPath);
					if(kvm.getStatus() == KVMessage.StatusType.PUT_ERROR){
						throw new Exception("PUT_NOT_SUCCESSFUL");
					}
					result = "PUT_REPLICATION_COMPLETE";
				} catch (Exception e) {
					logger.error(e.getMessage());
				}
			}
		}
		
		return result;
	}

	/**
	 * Retrieves the value for a given key from the KVServer.
	 *
	 * @param key the key that identifies the value.
	 * @return the value, which is indexed by the given key.
	 * @throws Exception if put command cannot be executed
	 *                   (e.g. not connected to any KV server).
	 */
	public KVMessage get(String key) throws Exception {
		KVMessage kvm;

		if (checkIfInRange(key, serverHashStart, serverHashEnd)) {
			logger.info("Getting from Main File the key: " + key);

			//add this request to the number of clients requesting list
			synchronized (lock) {
				clientsInRequest.add(0);
			}

			kvm = mStorage.get(key.trim(), "./data/storage"+port+".txt");

			//once done remove client's request from request list
			synchronized (lock) {
				clientsInRequest.remove(0);
			}
		} else {
			//check whether the server is the first or second replica, or not at all
			int index = metadata.indexOf(serverHashStart+","+serverHashEnd+","+ip+","+port+","+
			firstReplica.replaceAll(" ", ",")+","+secondReplica.replaceAll(" ", ","));
			
			int indexFR = (index - 1) < 0 ? (metadata.size() - 1) : (index-1);
			String[] fReplica = metadata.get(indexFR).split(",");
			
			int indexSR = (index - 2) < 0 ? (metadata.size() + (index-2)) : (index-2);
			String[] sReplica = metadata.get(indexSR).split(",");
			
			if(checkIfInRange(key, fReplica[0], fReplica[1])){
				//it is in the first replica file!
				logger.info("Getting from First Replica the key: " + key);

				//add this request to the number of clients requesting list
				synchronized (lock) {
					clientsInRequest.add(0);
				}

				synchronized(replicaLockFirst){
					kvm = mStorage.get(key.trim(), "./data/storage"+port+"FR.txt");
				}

				//once done remove client's request from request list
				synchronized (lock) {
					clientsInRequest.remove(0);
				}
			} else if(checkIfInRange(key, sReplica[0], sReplica[1])){
				//second replica file!
				logger.info("Getting from Second Replica the key: " + key);

				//add this request to the number of clients requesting list
				synchronized (lock) {
					clientsInRequest.add(0);
				}

				synchronized(replicaLockSecond){
					kvm = mStorage.get(key.trim(), "./data/storage"+port+"SR.txt");					
				}

				//once done remove client's request from request list
				synchronized (lock) {
					clientsInRequest.remove(0);
				}
			} else{
				//need to send updated metadata to client
				synchronized(metaLock){
					String bigData = metadata.get(0).trim();
					boolean skipFirst = false;

					for (String server : metadata) {
						if (skipFirst) {
							bigData = bigData + " " + server.trim();
						} else {
							skipFirst = true;
						}
					}
					kvm = new KVMessageStorage(bigData, "", KVMessage.StatusType.SERVER_NOT_RESPONSIBLE);
				}
			}
		}

		return kvm;
	}
	
	public synchronized KVMessage subscribe(String key, int portListen){
		KVMessage kvm = new KVMessageStorage("An error has occurred, please try again.", 
				"", KVMessage.StatusType.SUBSCRIBE_ERROR);
		
		if(checkIfInRange(key, serverHashStart, serverHashEnd)){
			try{
				//check if key exists in storage
				KVMessage kvStore = mStorage.get(key.trim(), "./data/storage"+port+".txt");
				if(kvStore.getStatus() != KVMessage.StatusType.GET_ERROR){
					
					//add this request to the number of clients requesting list
					synchronized (lock) {
						clientsInRequest.add(0);
					}
					synchronized (writeLock) {
						writeRequests.add(0);
					}
					
					//add subscription to the subscription file
					synchronized(subscriptLock){
						addPortToSubscription(key, portListen, "./data/subscriptions"+port+".txt", 
								"./data/tempSubscriptions"+port+".txt");
					}
					
					//once done remove client's request from request list
					synchronized (lock) {
						clientsInRequest.remove(0);
					}
					synchronized (writeLock) {
						writeRequests.remove(0);
					}
					
					kvm = new KVMessageStorage(key, kvStore.getValue(), KVMessage.StatusType.SUBSCRIBE_SUCCESS);
				} else {
					//notify client that key is not stored yet, nothing to subscribe to
					kvm = new KVMessageStorage("Key not stored yet, nothing to subscribe to.", 
							"", KVMessage.StatusType.SUBSCRIBE_ERROR);
				}
			} catch(Exception e){
				logger.error(e.getMessage());
			}
		} else {
			//need to send updated metadata to client
			synchronized(metaLock){
				String bigData = metadata.get(0).trim();
				boolean skipFirst = false;

				for (String server : metadata) {
					if (skipFirst) {
						bigData = bigData + " " + server.trim();
					} else {
						skipFirst = true;
					}
				}
				kvm = new KVMessageStorage(bigData, "", KVMessage.StatusType.SERVER_NOT_RESPONSIBLE);
			}
		}
		
		return kvm;
	}
	
	public void addPortToSubscription(String key, int port, String mainFile, String tempFile) throws Exception{
		//writer for the original file
        FileWriter write = new FileWriter(mainFile, true);
        PrintWriter printWrite = new PrintWriter(write);
        //writer for a temp file that could replace the original file
        FileWriter writeTemp = new FileWriter(tempFile, true);
        PrintWriter printTemp = new PrintWriter(writeTemp);
		
		//read file for this key, maybe its an update
        File inputFile = new File(mainFile);
        
        BufferedReader br = new BufferedReader(new FileReader(inputFile));

        String line;
        boolean replaced = false;

        while((line = br.readLine()) != null){
            if(line.length() != 0){
                String[] kv = line.split(" ", 2);
                if(kv[0].equals(key)){
                    //replace this line
                    replaced = true;
                    if(!portCheck(kv[1], port)){
                    	printTemp.println(key+" "+kv[1]+" "+port);
                    } else {
                    	printTemp.println(key+" "+kv[1]);
                    }
                }else{
                    printTemp.println(line);
                }
            }
        }

        printTemp.close();
        File temp = new File(tempFile);

        if(!replaced){
            //delete temp file
            logger.info("Temp Deletion: " + temp.delete());

            printWrite.println(key+" "+port);
            printWrite.close();
        } else {
            //delete original storage.txt and rename the temp file
            logger.info("Original Deletion: " + inputFile.delete());
            logger.info("Renaming of Temp: " + temp.renameTo(inputFile));
        }
        
        br.close();
	}
	
	public boolean portCheck(String value, int portListen){
		List<String> val = Arrays.asList(value.split(" "));
		
		return val.contains(String.valueOf(portListen));
	}
	
	public synchronized KVMessage unsubscribe(String key, int portListen){
		KVMessage kvm = new KVMessageStorage("An error has occurred, please try again.", 
				"", KVMessage.StatusType.UNSUBSCRIBE_ERROR);
		
		if(checkIfInRange(key, serverHashStart, serverHashEnd)){
			try{
				//check if key exists in storage
				KVMessage kvStore = mStorage.get(key.trim(), "./data/storage"+port+".txt");
				if(kvStore.getStatus() != KVMessage.StatusType.GET_ERROR){
					
					//add this request to the number of clients requesting list
					synchronized (lock) {
						clientsInRequest.add(0);
					}
					synchronized (writeLock) {
						writeRequests.add(0);
					}
					
					//remove subscription if in the subscription file
					synchronized(subscriptLock){
						removePortFromSubscription(key, portListen, "./data/subscriptions"+port+".txt", 
								"./data/tempSubscriptions"+port+".txt");
					}
					
					//once done remove client's request from request list
					synchronized (lock) {
						clientsInRequest.remove(0);
					}
					synchronized (writeLock) {
						writeRequests.remove(0);
					}
					
					kvm = new KVMessageStorage(key, "", KVMessage.StatusType.UNSUBSCRIBE_SUCCESS);
				} else {
					//notify client that key is not stored yet, nothing to subscribe to
					kvm = new KVMessageStorage("Key not stored yet, nothing to unsubscribe from.", 
							"", KVMessage.StatusType.UNSUBSCRIBE_ERROR);
				}
			} catch(Exception e){
				logger.error(e.getMessage());
			}
		} else {
			//need to send updated metadata to client
			synchronized(metaLock){
				String bigData = metadata.get(0).trim();
				boolean skipFirst = false;

				for (String server : metadata) {
					if (skipFirst) {
						bigData = bigData + " " + server.trim();
					} else {
						skipFirst = true;
					}
				}
				kvm = new KVMessageStorage(bigData, "", KVMessage.StatusType.SERVER_NOT_RESPONSIBLE);
			}
		}
		
		return kvm;
	}
	
	public void removePortFromSubscription(String key, int port, String mainFile, String tempFile) throws Exception{
		//writer for the original file
        FileWriter write = new FileWriter(mainFile, true);
        PrintWriter printWrite = new PrintWriter(write);
        //writer for a temp file that could replace the original file
        FileWriter writeTemp = new FileWriter(tempFile, true);
        PrintWriter printTemp = new PrintWriter(writeTemp);
        
        //delete the corresponding key value pair in the file
        File inputFile = new File(mainFile);
        
        BufferedReader br = new BufferedReader(new FileReader(inputFile));

        String line;
        boolean deleted = false;

        while((line = br.readLine()) != null){
            if(line.length() != 0){
                String[] kv = line.split(" ", 2);
                if(kv[0].equals(key)){
                    //delete the target port from this line
                    deleted = true;
                    String result = "";
                    for(String value : kv[1].split(" ")){
                    	if(!value.equals(String.valueOf(port))){
                    		result = result + value + " ";
                    	}
                    }
                    
                    result = result.trim();
                    
                    if(result.length() != 0){
                        printTemp.println(key + " " + result);                    	
                    }
                }else{
                    printTemp.println(line);
                }
            }
        }

        File temp = new File(tempFile);

        if(!deleted){
            //delete temp file
            logger.error("Temp Deletion: " + temp.delete());
        } else {
            //delete original storage.txt and rename the temp file
            logger.info("Original Deletion: " + inputFile.delete());
            logger.info("Renaming of Temp: " + temp.renameTo(inputFile));
        }
        
        printTemp.close();
        printWrite.close();
        br.close();
	}
	
	public void tellSubscribers(String key, String value, String mainFile){
		try{
			//find the key in the following file
	        File inputFile = new File(mainFile);
	        
	        BufferedReader br = new BufferedReader(new FileReader(inputFile));

	        String line;
	        boolean foundIt = false;

	        while((line = br.readLine()) != null){
	            if(line.length() != 0){
	                String[] kv = line.split(" ", 2);
	                if(kv[0].equals(key)){
	                	line = kv[1];
	                	foundIt = true;
	                	break;
	                }
	            }
	        }
	        
	        if(foundIt){
	        	String[] ports = line.trim().split(" ");
	        	for(String port : ports){
	        		//only set up for localhost transactions
	        		Socket servToServ = new Socket("127.0.0.1", Integer.parseInt(port.trim()));
	        		
	    			//get input/output stream
	    			PrintStream out = new PrintStream(servToServ.getOutputStream(), true);
	    			BufferedReader in = new BufferedReader(new InputStreamReader(servToServ.getInputStream()));
	    			
	    			if(value == null){ 
	    				value = "";
    				}
	    			
	    			out.println("change " + key + " " + value);
	    			in.readLine();
	    			
	    			servToServ.close();
	        	}
	        }
		} catch(Exception e){
			logger.error(e.getMessage());
		}
	}

	public boolean checkIfInRange(String key, String start, String end) {
		String keyHash = hashFunction(key.trim());
		
		int biggerThan = keyHash.compareTo(start);
		int lessThan = keyHash.compareTo(end);
		int condition = start.compareTo(end);

		if (biggerThan > 0 && lessThan <= 0 && condition < 0) { //-------------------CHECK THIS--------------//
			return true;
		} else if (biggerThan > 0 && condition > 0) {
			return true;
		} else if (lessThan <= 0 && condition > 0) {
			return true;
		} else if (condition == 0) {
			return true;
		} else {
			return false;
		}
	}

	//****************************ADMIN ECS REQUEST METHODS****************************//

	public KVAdminMessage initKVServer(String[] metadata, int cacheSize, String replacementStrategy) {
		//parse metadata and store in
		logger.info("Initializing Server: No Client Requests Allowed");
		
		try {
			String serverAddr = InetAddress.getLocalHost().getHostAddress();
			logger.info("Server is bound to IP: " + serverAddr);
			
			for (String server : metadata) {
				this.metadata.add(server.trim());

				//set the server's hash range
				String[] serverSplit = server.split(",");
				if ((serverSplit[2].equals(serverAddr) || serverSplit[2].equals("127.0.0.1")) && Integer.parseInt(serverSplit[3]) == port) {
					logger.info("MATCH FOUND");

					serverHashStart = serverSplit[0];
					serverHashEnd = serverSplit[1];
					ip = serverSplit[2];
					firstReplica = serverSplit[4] + " " + serverSplit[5];
					secondReplica = serverSplit[6] + " " + serverSplit[7];
				}
			}
			
			//sort the metadata list lexicographically in order to find first and second replicas easily
			Collections.sort(this.metadata);
			
			//initialize the storage system
			mStorage = new storageServer(replacementStrategy.toUpperCase(), cacheSize, port);
			
			//create the file that will store data for this server
			File outputFile = new File("./data/storage"+port+".txt");
			
			if(!outputFile.exists()){
				outputFile.getParentFile().mkdirs();
				outputFile.createNewFile();
			}
			
			new PrintWriter(outputFile).close();
			
			//create the file that will store data for the first replica
			outputFile = new File("./data/storage"+port+"FR.txt");
			
			if(!outputFile.exists()){
				outputFile.createNewFile();
			}
			
			new PrintWriter(outputFile).close();
			
			//create the file that will store data for the second replica
			outputFile = new File("./data/storage"+port+"SR.txt");
			
			if(!outputFile.exists()){
				outputFile.createNewFile();
			}
			
			new PrintWriter(outputFile).close();
			
			//create the file that will store subscriptions for this server
			outputFile = new File("./data/subscriptions"+port+".txt");
			
			if(!outputFile.exists()){
				outputFile.getParentFile().mkdirs();
				outputFile.createNewFile();
			}
			
			new PrintWriter(outputFile).close();
			
		} catch (IOException e) {
			logger.error(e.getMessage());			
		}

		return new KVAdminMessageStorage(KVAdminMessage.StatusType.INITIALIZATION_SUCCESS, "");
	}

	public KVAdminMessage start() {
		logger.info("Starting Server: Client Requests Allowed");
		state = SERVER_READY;
		return new KVAdminMessageStorage(KVAdminMessage.StatusType.SERVER_STARTED, "");
	}

	public KVAdminMessage stop() {
		logger.info("Stopping Server: No Client Requests Allowed");
		state = SERVER_STOPPED;

		boolean stopPossible = false;

		while (!stopPossible) {
			synchronized (lock) {
				if (clientsInRequest.size() == 0) {
					stopPossible = true;
				}
			}
		}

		return new KVAdminMessageStorage(KVAdminMessage.StatusType.SERVER_STOPPED, "");
	}

	public KVAdminMessage shutDown() {
		logger.info("Shutting Down Server: Will Process Current Requests Before Exiting");

		state = SERVER_STOPPED;

		boolean sdPossible = false;

		while (!sdPossible) {
			synchronized (lock) {
				if (clientsInRequest.size() == 0) {
					sdPossible = true;
				}
			}
		}

		logger.info("All Requests Complete, Sending data from file to ECS");
		
		//Access file, store data to a string, then send data, delete file
		String data = "";
		try{
			File inputFile = new File("./data/storage" + port + ".txt");
			
			if(inputFile.exists()){
				BufferedReader br = new BufferedReader(new FileReader(inputFile));

				String line;

				//read every line of the file for each key value pair
				while ((line = br.readLine()) != null) {
					if (line.length() != 0) {
						String[] kv = line.split(" ", 2);
						data = data + kv[0] + "," + kv[1].replaceAll(" ", "-") + " ";
					}
				}
				
				br.close();
				inputFile.delete();
			}
			
			//delete firstReplica storage file
			inputFile = new File("./data/storage" + port + "FR.txt");
			if(inputFile.exists()){
				inputFile.delete();
			}
			
			//delete secondReplica storage file
			inputFile = new File("./data/storage" + port + "SR.txt");
			if(inputFile.exists()){
				inputFile.delete();
			}
			
			//delete subscription file
			inputFile = new File("./data/subscriptions"+port+".txt");
			if(inputFile.exists()){
				inputFile.delete();
			}
			
		} catch(FileNotFoundException e){
			logger.error(e.getMessage());
		} catch(IOException e){
			logger.error(e.getMessage());
		}
		
		if(data.equals("")){
			return new KVAdminMessageStorage(KVAdminMessage.StatusType.SERVER_SHUTDOWN_COMPLETE, "");
		} else {
			return new KVAdminMessageStorage(KVAdminMessage.StatusType.SERVER_SHUTDOWN_COMPLETE, data.trim());
		}
	}

	public KVAdminMessage lockWrite() {
		//cannot notify the ECS Server until every ongoing write request is complete
		//setting banWrite will prevent other clients from making a PUT Request
		logger.info("Banning Put Operations: Setting Lock Variable");
		banWrite = BAN_WRITE;
		boolean lockPossible = false;

		while (!lockPossible) {
			synchronized (writeLock) {
				if (writeRequests.size() == 0) {
					lockPossible = true;
				}
			}
		}

		return new KVAdminMessageStorage(KVAdminMessage.StatusType.LOCK_WRITE_SUCCESSFUL, "");
	}

	public KVAdminMessage unLockWrite() {
		logger.info("Allowing Put Operations by Clients");
		banWrite = ALLOW_WRITE;
		return new KVAdminMessageStorage(KVAdminMessage.StatusType.UNLOCK_WRITE_SUCCESSFUL, "");
	}

	public KVAdminMessage moveData(String range, String targetServer) { 
		//if the data transfer is successful the return value result will be modified
		KVAdminMessage result = new KVAdminMessageStorage(KVAdminMessage.StatusType.DATA_TRANSFER_FAILED, "");

		try {
			String[] rangeSplit = range.split("-");
			String[] AddrPortSplit = targetServer.split("-");

			//this list will hold all the key value pairs to be copied over
			List<String> kvList = new ArrayList<String>();

			//attain the pair of key-value pairs that are within the range
			File inputFile = new File("./data/storage" + port + ".txt");
			
			if(!inputFile.exists()){
				inputFile.createNewFile();
			}
			
			BufferedReader br = new BufferedReader(new FileReader(inputFile));

			String line;

			//read every line of the file for each key value pair
			while ((line = br.readLine()) != null) {
				if (line.length() != 0) {
					String[] kv = line.split(" ", 2);
					if (checkIfInRange(kv[0], rangeSplit[0], rangeSplit[1])) {
						kvList.add(kv[0] + "," + kv[1].trim().replaceAll("\\s","-"));
					}
				}
			}

			//convert to a string so the data can be passed to the appropriate server through a output stream
			String kvData = "";
			for(String keyVal : kvList){
				kvData = kvData + keyVal + " ";
			}
			kvData = kvData.trim();
			
			if(!kvData.equals("")){
				//send the data to the server
				logger.info("Sending data to: " + AddrPortSplit[0] + " " + AddrPortSplit[1]);
				Socket servToServ = new Socket(AddrPortSplit[0].trim(), Integer.parseInt(AddrPortSplit[1].trim()));
	
				//get input/output stream
				PrintStream out = new PrintStream(servToServ.getOutputStream(), true);
				BufferedReader in = new BufferedReader(new InputStreamReader(servToServ.getInputStream()));
	
				//send the data to the server
				logger.info("Data to be sent: " + kvData);
				out.println("server addkvpairs " + kvData);
	
				String reply = in.readLine();
				if(reply.equals("DATA_TRANSFER_COMPLETE")){
					result = new KVAdminMessageStorage(KVAdminMessage.StatusType.DATA_TRANSFER_SUCCESSFUL, kvData);
				}
	
				out.close();
				in.close();
				servToServ.close();
			} else {
				logger.info("No data to transfer!");
				kvData = "";
				result = new KVAdminMessageStorage(KVAdminMessage.StatusType.DATA_TRANSFER_SUCCESSFUL, kvData);
			}
				
			br.close();

		} catch (Exception e) {
			logger.error(e.getMessage());
		}

		return result;
	}

	public KVAdminMessage deletePairs(String[] kvList){
		KVAdminMessage result = new KVAdminMessageStorage(KVAdminMessage.StatusType.DELETING_KVPAIRS_SUCCESSFUL, "");

		//for every string that holds a key-value pair in the list of key-value pairs, execute put to perform a delete of the key
		for(String kvPair : kvList){
			String key = kvPair.split(",")[0];

			try {
				KVMessage kvM = mStorage.put(key, null, "./data/storage"+port+".txt", "./data/temp"+port+".txt");
				logger.info(String.valueOf(kvM.getStatus()));
			} catch (Exception e) {
				logger.error(e.getMessage());
				result = new KVAdminMessageStorage(KVAdminMessage.StatusType.DELETING_KVPAIRS_FAILED, "");
				break;
			}
		}

		return result;
	}

	public String addKVPairs(String[] kvList){
		//get access to the file, or create it if first time through a writer
		try {
			File outputFile = new File("./data/storage"+port+".txt");
			FileWriter write = new FileWriter(outputFile, true);
			PrintWriter printWrite = new PrintWriter(write);

			//take each key-value pair and add it to the server's database file
			for(String kvPair : kvList){
				String key = kvPair.split(",", 2)[0];
				String value = kvPair.split(",", 2)[1].replaceAll("-", " ");

				printWrite.println(key + " " + value);
			}

			printWrite.close();
			write.close();
		} catch (IOException e) {
			logger.error(e.getMessage());
		}

		return "DATA_TRANSFER_COMPLETE";
	}
	
	public KVAdminMessage replicate(){
		KVAdminMessage kvAM = new KVAdminMessageStorage(KVAdminMessage.StatusType.REPLICATION_FAILED, "");
				
		try{
			//store data in a string to send to replica servers
			File inputFile = new File("./data/storage" + port + ".txt");
			
			if(!inputFile.exists()){
				inputFile.createNewFile();
			}
			
			BufferedReader br = new BufferedReader(new FileReader(inputFile));

			String line;
			String kvData = "";

			//read every line of the file for each key value pair
			while ((line = br.readLine()) != null) {
				if (line.length() != 0) {
					String[] kv = line.split(" ", 2);
					kvData = kvData + kv[0] + "," + kv[1].trim().replaceAll(" ", "-") + " ";
				}
			}
			
			//send all key value pairs to replicas
			String result;
			
			//first replica
			String[] fIpPort = firstReplica.split(" ");
			result = sendReplicaData(fIpPort[0], Integer.parseInt(fIpPort[1]), kvData.trim(), 1);
			if(! result.equals("REPLICATION_COMPLETE")){
				br.close();
				throw new IOException("Replication of data to first replica failed!");
			}
			
			logger.info("Data replicated into first replica");
			
			//second replica
			String[] sIpPort = secondReplica.split(" ");
			result = sendReplicaData(sIpPort[0], Integer.parseInt(sIpPort[1].trim()), kvData.trim(), 2);
			if(! result.equals("REPLICATION_COMPLETE")){
				br.close();
				throw new IOException("Replication of data to second replica failed!");
			}

			logger.info("Data replicated into second replica");
							
			
			kvAM = new KVAdminMessageStorage(KVAdminMessage.StatusType.REPLICATION_SUCCESSFUL, "");
			br.close();
		} catch(Exception e){
			logger.error(e.getMessage());
		}
		
		return kvAM;
	}
	
	private String sendReplicaData(String ip, int port, String kvData, int indicator){
		//open a socket to the first replica, and send a addReplicaData command with the data
		String reply = "";
		Socket servToServ;
		try {
			servToServ = new Socket(ip, port);
			
			//get input/output stream
			PrintStream out = new PrintStream(servToServ.getOutputStream(), true);
			BufferedReader in = new BufferedReader(new InputStreamReader(servToServ.getInputStream()));

			//send the data to the server
			logger.info("Data to be sent: " + kvData);
			
			//indicator used to identify whether request is sent by replica one or two
			out.println("server addreplicadata " + indicator + " " + kvData);

			reply = in.readLine();
			
			servToServ.close();
		} catch (Exception e) {
			logger.error(e.getMessage());
		} 
		
		return reply;
	}
	
	public String addReplicaData(int indicator, String[] kvData){
		String result = "REPLICATION_FAILED";
		
		String filePath = ((indicator == 1) ? "./data/storage"+port+"FR.txt" : "./data/storage"+port+"SR.txt");
		Object replicaLock = ((indicator == 1) ? replicaLockFirst : replicaLockSecond);
		
		try{
			//copy data to the appropriate replica indicated by the indicator (if indicator is wrong, don't do anything)
			if(indicator < 3){
				if(!kvData[0].equals("")){
					synchronized(replicaLock){
						File outputFile = new File(filePath);
						FileWriter write = new FileWriter(outputFile);
						PrintWriter printWrite = new PrintWriter(write);
						
						for (String keyValue : kvData){
							String[] kv = keyValue.split(",", 2);
							printWrite.println(kv[0] + " " + kv[1].replaceAll("-", " ").trim());
						}
						
						result = "REPLICATION_COMPLETE";
						printWrite.close();
					}
				} else{
					logger.info("No data to replicate, just empty file");
					
					File outputFile = new File(filePath);
					FileWriter write = new FileWriter(outputFile);
					PrintWriter printWrite = new PrintWriter(write);
					
					printWrite.close();
					
					result = "REPLICATION_COMPLETE";
				}
			}
		} catch(Exception e){
			logger.error(e.getMessage());
		}
		
		return result;
	}
	
	public KVAdminMessage firstReplicaDead(){
		//need to move data from the first replica file to the main storage file
		KVAdminMessage kvAM = new KVAdminMessageStorage(KVAdminMessage.StatusType.MOVING_REPLICA_DATA_TO_MAIN_FILE_FAILED, "");
		
		try {
			//writer for the main file
			FileWriter write = new FileWriter("./data/storage"+port+".txt", true);
			PrintWriter printWrite = new PrintWriter(write);
			
			//reader for the first replica file
            File inputFile = new File("./data/storage"+port+"FR.txt");
            BufferedReader br = new BufferedReader(new FileReader(inputFile));

            String line;

            while((line = br.readLine()) != null){
                if(line.length() != 0){
                    String[] kv = line.split(" ");
                    printWrite.println(kv[0] + " " + kv[1]);
                }
            }
            
            printWrite.close();
            br.close();
            
            kvAM = new KVAdminMessageStorage(KVAdminMessage.StatusType.MOVING_REPLICA_DATA_TO_MAIN_FILE_SUCCESSFUL, "");
		} catch (IOException e) {
			logger.error(e.getMessage());
		}
		
		return kvAM;
	}

	/**
     * Hashes a string value and returns an integer
     * to be put on the circle
     * @param ipAndPort IP + Port number of a server
     * @return Hashed Value
     */
    public String hashFunction(String ipAndPort){

        try {
            MessageDigest md = null;
            md = MessageDigest.getInstance("MD5");
            md.update(ipAndPort.replaceAll("\\s","").getBytes());
            byte[] digest = md.digest();
            String hash = toHex(digest);
            
            logger.info("original: " + ipAndPort);
            logger.info("digested(hex): " + hash);
            return hash;
        } catch (NoSuchAlgorithmException e) {
            logger.error(e.getMessage());
            return null;
        }

    }
    
    public String toHex(byte[] bytes) {
        BigInteger bi = new BigInteger(1, bytes);
        return String.format("%0" + (bytes.length << 1) + "x", bi);
    }

	public KVAdminMessage update(String[] metadata) {
		logger.info("Updating metadata with new metadata from the ECS Server");

		try {
			//clear metadata and update metadata
			synchronized (metaLock) {
				this.metadata.clear();
				
				//get IP address of server
				String serverAddr = InetAddress.getLocalHost().getHostAddress();
				
				for (String server : metadata) {
					this.metadata.add(server.trim());
					//logger.error("From " + port + " the server is in metadata " + server);
					
					//set the server's hash range
					String[] serverSplit = server.split(",");
					if ((serverSplit[2].equals(serverAddr) || serverSplit[2].equals("127.0.0.1")) && Integer.parseInt(serverSplit[3]) == port) {
						logger.info("MATCH FOUND");

						serverHashStart = serverSplit[0];
						serverHashEnd = serverSplit[1];
						ip = serverSplit[2];
						firstReplica = serverSplit[4] + " " + serverSplit[5];
						secondReplica = serverSplit[6] + " " + serverSplit[7];
					}
				}
				
				//sort the metadata list lexicographically in order to find first and second replicas easily
				Collections.sort(this.metadata);
			}
		} catch (Exception e) {
			logger.error(e.getMessage());
			return new KVAdminMessageStorage(KVAdminMessage.StatusType.METADATA_UPDATE_FAILED, "");
		}

		return new KVAdminMessageStorage(KVAdminMessage.StatusType.METADATA_UPDATE_SUCCESSFUL, "");
	}

	//****************************SERVER METHODS****************************//

	private boolean initializeServer() {
		logger.info("Setting up server socket ...");
		try {
			serverSocket = new ServerSocket(port);
			logger.info("Server listening on port: "
					+ serverSocket.getLocalPort());
			return true;

		} catch (IOException e) {
			logger.error("Error! Cannot open server socket:");
			if (e instanceof BindException) {
				logger.error("Port " + port + " is already bound!");
			}
			return false;
		}
	}

	/**
	 * Main entry point for the echo server application.
	 *
	 * @param args contains the port number at args[0].
	 */
	public static void main(String[] args) {
		try {
			if (args.length != 3) {
				System.out.println("Error! Invalid number of arguments!");
				System.out.println("Usage: Server <port> <ecsAddr> <ecsPort>");
			} else {

				int port = Integer.parseInt(args[0]);
				String ecsAddr = args[1];
				int ecsPort = Integer.parseInt(args[2]);

				if(ecsAddr.equals("null") && ecsPort < 1){
					KVServer kvs = new KVServer(port, null, -1);
					new Thread(kvs).start();
				} else {
					KVServer kvs = new KVServer(port, ecsAddr, ecsPort);
					new Thread(kvs).start();
				}

			}
		} catch (NumberFormatException nfe) {
			System.out.println("Error! Please check whether the <port> and <ecsPort> is a number");
			System.out.println("Usage: Server <port> <ecsAddr> <ecsPort>");
			System.exit(1);
		}
	}

	public storageServer getStorageServer() {
		return mStorage;
	}
	
}
