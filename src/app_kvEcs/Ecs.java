package app_kvEcs;

import client.TextMessage;
import org.apache.log4j.Logger;

import java.io.*;
import java.net.InetAddress;
import java.net.InetSocketAddress;
import java.net.ServerSocket;
import java.net.Socket;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Random;

public class Ecs {

    public List<String> mIpAndPorts;
    private static final String PROMPT = "ECS> ";
    private static final int ecsPort = 40000;
    
    private BufferedReader stdin;
    private boolean stop = false;
    private static final int BUFFER_SIZE = 1024;
    private static final int DROP_SIZE = 1024*BUFFER_SIZE;
    private static Logger logger = Logger.getRootLogger();
    
    public List<String> participatingServers;
    public List<String> notParticipating;
    public List<String> kvList;
    public List<HeartBeatThread> hbThreadList;
    public ConsistentHashing cs = null;
    public String metadata = null;
    
    public static final String FAIL_COMMAND = "ecs fail";
    
    public boolean pauseOnFailure = false;

    public void run() {
    	//get all servers within the config file into a accessible data structure
    	readFile();
    	
    	try{
    		//get all values from persisted storage file
        	File outputFile = new File("./data/storage.txt");
    		
    		if(!outputFile.exists()){
    			outputFile.getParentFile().mkdirs();
    			outputFile.createNewFile();
    		}
    		
    		BufferedReader br = new BufferedReader(new FileReader(outputFile));
    		
    		kvList = new ArrayList<String>();
    		
			String line;

			//read every line of the file for each key value pair
			while ((line = br.readLine()) != null) {
				if (line.length() != 0) {
					String[] kv = line.split(" ", 2);
					kvList.add(kv[0] + "," + kv[1]);
				}
			}
			
			br.close();
    		
    	} catch(IOException e){
    		logger.error(e.getMessage());
    	}
    	
        while(!stop) {
            stdin = new BufferedReader(new InputStreamReader(System.in));
            System.out.print(PROMPT);
            
            try {
                String cmdLine = stdin.readLine();
                
                //dont handle the command until pauseOnFailure is set to false, indicates failure has occurred.
                while(pauseOnFailure){}
                
                this.handleCommand(cmdLine);
            } catch (Exception e) {
                stop = true;
                System.out.println("CLI does not respond - Application terminated");
            }
    	}
    }

    private void handleCommand(String cmdline){

        String[] tokens = cmdline.split("\\s+");

        if(tokens[0].equals("quit")) {
            stop = true;
            //	disconnect();
            System.out.println(PROMPT + "Application exit!");
        } else if (tokens[0].equals("initservice")){
            if(tokens.length == 4) {
            	try {
	                int numberOfNodes = Integer.parseInt(tokens[1]);
	                int cacheSize = Integer.parseInt(tokens[2]);
	                String replacementStrategy = tokens[3];
	                
	                initService(numberOfNodes, cacheSize, replacementStrategy);
                } catch (NumberFormatException e){
                	printError(e.getMessage());
                	System.out.println("Usage: initservice <numberOfNodes> <cacheSize> <replacementStrategy>");
                }
            }else{
            	printError("Invalid number of parameters!");
            	System.out.println("Usage: initservice <numberOfNodes> <cacheSize> <replacementStrategy>");
            }
        } else if (tokens[0].equals("start")){
            if (tokens.length == 1){
                start();
            } else {
            	printError("Invalid number of parameters!");
            	System.out.println("Usage: start");
            }
        } else if (tokens[0].equals("stop")){
            if (tokens.length == 1){
                stop();
            } else {
            	printError("Invalid number of parameters!");
            	System.out.println("Usage: stop");
            }
        } else if (tokens[0].equals("shutdown")){
            if (tokens.length == 1){
                shutdown();
            } else {
            	printError("Invalid number of parameters!");
            	System.out.println("Usage: shutdown");
            }
        } else if (tokens[0].equals("addnode")){
            if (tokens.length == 3){
            	int cacheSize = Integer.valueOf(tokens[1]);
            	String replacementStrategy = tokens[2];
            	
                addNode(cacheSize, replacementStrategy);
            } else {
            	printError("Invalid number of parameters!");
            	System.out.println("Usage: addnode <cacheSize> <replacementStrategy>");
            }
        } else if (tokens[0].equals("deletenode")) {
        	if (tokens.length == 2){
        		int servNum = Integer.valueOf(tokens[1]);
        		deleteNode(servNum);
        	} else {
        		printError("Invalid number of parameters!");
            	System.out.println("Usage: addnode <serverNumber>");
        	}
        } else if (tokens[0].equals("fail")) {
			System.out.println("SHUTDOWN --- calling tester Shutdown to stop a server "+ "127.0.0.1" + tokens[2]);
			testerShutdown("127.0.0.1",tokens[2]);
		} else {
        	printError("Command not valid!");
        }
    }

    public void start(){
        logger.info("Number of participating servers: " + participatingServers.size());
        logger.info("Servers will be started");

        for (String server : participatingServers){
            String[] elements = server.split(" ");
            String ip = elements[0];
            String port = elements[1];
            try {
                connect(ip,Integer.valueOf(port),"ecs start");
            } catch (Exception e) {
                logger.error(e.getMessage());
            }
        }

    }
    
    public void stop(){
        logger.info("Number of participating servers: " + participatingServers.size());
        logger.info("Servers will be stopped");

        for (String server : participatingServers){
            String[] elements = server.split(" ");
            String ip = elements[0];
            String port = elements[1];
            try {
                connect(ip,Integer.valueOf(port),"ecs stop");
            } catch (Exception e) {
                logger.error(e.getMessage());
            }
        }

    }
    
    public void shutdown(){
    	//remove each node, get data from node to store into storage.txt
    	logger.info("Number of participating servers: " + participatingServers.size());
        logger.info("Servers will be shutdown, all data will be stored by the ECS");
        
        try {
        	//empty the storage.txt file as it is receiving new data
        	File emptyFile = new File("./data/storage.txt");
			new PrintWriter(emptyFile).close();
			
			//empty kvList, as it will receive fresh new data
			kvList.clear();
			
			for (String server : participatingServers){
	            String[] elements = server.split(" ");
	            String ip = elements[0];
	            String port = elements[1];
	            try {
	            	String result = connect(ip, Integer.valueOf(port), "ecs shutdown");
	            	
					if(!result.equals("")){
		            	String[] kvData = result.split(" ");
		            	
		            	File outputFile = new File("./data/storage.txt");
		    			FileWriter write = new FileWriter(outputFile, true);
		    			PrintWriter printWrite = new PrintWriter(write);
		
		    			//take each key-value pair and add it to the server's database file
		    			for(String kvPair : kvData){
		    				String key = kvPair.split(",")[0];
		    				String value = kvPair.split(",")[1].replaceAll("-", " ");
		
		    				printWrite.println(key + " " + value);
		    				kvList.add(key + "," + value);
		    			}
		
		    			printWrite.close();
		    			write.close();            		
	            	}						
	            } catch (Exception e) {
	                logger.error(e.getMessage());
	            }
	        }
			
			if(hbThreadList != null){
				int size = hbThreadList.size();
				for(int i = 0; i < size; i++){
					HeartBeatThread hb = hbThreadList.get(i);
	                hb.failureDetected = true;
				}
			}
		} catch (FileNotFoundException e1) {
			logger.error(e1.getMessage());
		}
        
        //reset cs, and participating/notParticipating global variables
        cs = null;
        metadata = null;
        participatingServers.clear();
        notParticipating.clear();
        
        if(hbThreadList != null){
        	hbThreadList.clear();
        }
        
    }
    
    /**
	 * Shutdown one server for testing failures
	 * @param ipAddress
	 * @param port
	 */
	public void testerShutdown(String ipAddress, String port){
		try {
			connect(ipAddress.trim(), Integer.valueOf(port), FAIL_COMMAND);
		} catch (Exception e) {
			logger.error("testerShutdown FAILED!");
		}
	}
    
    public void addNode(int cacheSize, String replacementStrategy){
    	logger.info("Number of participating servers: " + participatingServers.size());
    	if(participatingServers.size() != mIpAndPorts.size()){
    		Random rand = new Random();
    		int index = rand.nextInt(notParticipating.size());
    		
    		//add the server at index to the hashCircle
    		logger.info("Adding new server to hashCircle and participatingServers list");
    		cs.add(notParticipating.get(index));
    		participatingServers.add(notParticipating.get(index));
    		
    		//get required information to move data allocated to the new server
    		String[] ipAndport = notParticipating.get(index).split(" ");
            String ipAddr = ipAndport[0];
            String portNum = ipAndport[1];

            ConsistentHashing.HashedServer hS = cs.get(notParticipating.get(index));
            String startHash = hS.mHashedKeys[0].trim();
            String endHash = hS.mHashedKeys[1].trim();
            
            //we will send the moveData request to this IP and Port server
            String targetIp = null;
            String targetPort = null;
            
            //update metadata and find previous server to the new server on circle
    		logger.info("Updating metadata with new server");
    		StringBuilder sb = new StringBuilder("");
    		
    		for (String ipAndPort : participatingServers){
                
        		try{
        			String[] elements= ipAndPort.split(" ");
	                String ip = elements[0];
	                String port =elements[1];
	
	                ConsistentHashing.HashedServer hashedServer= cs.get(ipAndPort);
	                String start = hashedServer.mHashedKeys[0].trim();
	                String end = hashedServer.mHashedKeys[1].trim();
	                String[] replicaAddr = cs.getReplicas(end);
	                
	                String[] firstIpPort = replicaAddr[0].split(" ");
	                String firstIP = firstIpPort[0];
	                String firstPort = firstIpPort[1];
	                
	                String[] secondIpPort = replicaAddr[1].split(" ");
	                String secondIP = secondIpPort[0];
	                String secondPort = secondIpPort[1];
	                
	                if(start.equals(endHash)){
	                	targetIp = ip;
	                	targetPort = port;
	                }
	                
	                //FORMAT: startingHash,endingHash,ip,port,firstIP,firstPort,secondIP,secondPort 
	                sb.append(start.trim()).append(",").append(end.trim()).append(",")
	                .append(ip.trim()).append(",").append(port.trim())
	                .append(",").append(firstIP).append(",").append(firstPort).append(",")
	                .append(secondIP).append(",").append(secondPort).append(" ");    
        		} catch(Exception e){
        			logger.error(e.getMessage());
        		}	
        	}
    		
    		metadata = sb.toString().trim();
        	logger.debug("The metadata: " + metadata);
            
            //initiate the server
            logger.info("ip passed in: "+ipAddr+ " port: "+portNum);
			
			ssh(ipAddr,portNum,"ecs initkvserver " + cacheSize + " " + replacementStrategy + " " + metadata);
			
			try {
				//start the new server
				connect(ipAddr, Integer.valueOf(portNum), "ecs start");
				
				//writeLock the new server
				connect(ipAddr, Integer.valueOf(portNum), "ecs lockwrite");
				
				//writeLock target server
				connect(targetIp, Integer.valueOf(targetPort), "ecs lockwrite");
				
				logger.info("Both servers locked");
				
				//send moveData command to target server
				logger.info("Moving data from target server to new server");
				String command = "ecs movedata " + startHash + "-" + endHash + " " + ipAddr + "-" + portNum;
				String deleteData = connect(targetIp, Integer.valueOf(targetPort), command);
				
				//update metadata of all servers
				updateAllServers(metadata);
				logger.info("Metadata updated for all servers");
				logger.debug("metadata check: " + metadata);
				
				//unlock write permission for the new server
				connect(ipAddr, Integer.valueOf(portNum), "ecs unlockwrite");
				
				//unlock write permission for the target server
				connect(targetIp, Integer.valueOf(targetPort), "ecs unlockwrite");
				
				logger.info("Write permission given");
				
				if(!deleteData.trim().equals("")){
					//delete remaining key-value pairs from target server that dont matter
					connect(targetIp, Integer.valueOf(targetPort), "ecs deletepairs " + deleteData);
					
					logger.info("KV pairs deleted from target server");
				} else {
					logger.info("No KV pairs to delete");
				}
				
				logger.info("Starting HeartBeat thread for: " + ipAddr + " " + portNum);
				HeartBeatThread hbThread = new HeartBeatThread(ipAddr + " " + portNum);
				hbThreadList.add(hbThread);
				hbThread.start();
				
				//write lock all servers
				logger.info("All servers being write locked to initiate replication");
				writeLockAll();
				
				//initiate replication in all servers
				logger.info("Replicate all data in every server due to new added node");
				initiateReplication();
				
				//allowing writing of data in all servers
				logger.info("All servers are being given write permission - replication complete");
				writeUnlockAll();
				
			} catch (Exception e) {
				logger.error(e.getMessage());
			}
            
            notParticipating.remove(index);
            logger.info("Number of participating servers after addition: " + participatingServers.size());
    	} else {
    		printError("All available servers have been added!");
    	}
    }
    
    public void writeLockAll(){
    	for (String ipAndPort : participatingServers){
    		String[] elements= ipAndPort.split(" ");
            String ip = elements[0];
            String port =elements[1];
            
            try {
				connect(ip, Integer.parseInt(port), "ecs lockwrite");
			} catch (Exception e) {
				logger.error(e.getMessage());
			}
    	}
    }
    
    public void writeUnlockAll(){
    	for (String ipAndPort : participatingServers){
    		String[] elements= ipAndPort.split(" ");
            String ip = elements[0];
            String port =elements[1];
            
            try {
				connect(ip, Integer.parseInt(port), "ecs unlockwrite");
			} catch (Exception e) {
				logger.error(e.getMessage());
			}
    	}
    }
    
    public void updateAllServers(String metadata){
    	for(String ipAndPort : participatingServers){
    		String[] elements= ipAndPort.split(" ");
            String ip = elements[0];
            String port =elements[1];
            
            try {
				connect(ip, Integer.valueOf(port), "ecs update " + metadata);
			} catch (Exception e) {
				logger.error(e.getMessage());
			}
    	}
    }
    
    public void deleteNode(int servNum){
    	logger.info("Number of participating servers: " + participatingServers.size());
    	if(participatingServers.contains(mIpAndPorts.get(servNum - 1))){
    		if(participatingServers.size() > 3){
    			//get required information to move data allocated to an existing server
        		String[] ipAndport = mIpAndPorts.get(servNum - 1).split(" ");
                String ipAddr = ipAndport[0];
                String portNum = ipAndport[1];

                ConsistentHashing.HashedServer hS = cs.get(mIpAndPorts.get(servNum - 1));
                String startHash = hS.mHashedKeys[0].trim();
                String endHash = hS.mHashedKeys[1].trim();
                
                //we will send the moveData request to this IP and Port server
                String targetIp = null;
                String targetPort = null;
                
                //remove the server from participatingServers and the HashCircle and close its heartbeat thread
                int remIndex = participatingServers.indexOf(ipAddr + " " + portNum);
                participatingServers.remove(remIndex);
                
                HeartBeatThread hb = hbThreadList.get(remIndex);
                hb.failureDetected = true;
                hbThreadList.remove(remIndex);
                
                cs.remove(ipAddr + " " + portNum);
                
                //update metadata and find server that is right after deleted server on circle
        		logger.info("Updating metadata without the deleted server");
        		StringBuilder sb = new StringBuilder("");
        		
        		for (String ipAndPort: participatingServers){
                    
            		try{
            			String[] elements= ipAndPort.split(" ");
    	                String ip = elements[0];
    	                String port =elements[1];
    	
    	                ConsistentHashing.HashedServer hashedServer= cs.get(ipAndPort);
    	                String start = hashedServer.mHashedKeys[0].trim();
    	                String end = hashedServer.mHashedKeys[1].trim();
    	                String[] replicaAddr = cs.getReplicas(end);
    	                
    	                String[] firstIpPort = replicaAddr[0].split(" ");
    	                String firstIP = firstIpPort[0];
    	                String firstPort = firstIpPort[1];
    	                
    	                String[] secondIpPort = replicaAddr[1].split(" ");
    	                String secondIP = secondIpPort[0];
    	                String secondPort = secondIpPort[1];
    	                
    	                if(start.equals(startHash)){
    	                	targetIp = ip;
    	                	targetPort = port;
    	                	logger.info("Found target server");
    	                }
    	                
    	                //FORMAT: startingHash,endingHash,ip,port,firstIP,firstPort,secondIP,secondPort 
    	                sb.append(start.trim()).append(",").append(end.trim()).append(",")
    	                .append(ip.trim()).append(",").append(port.trim())
    	                .append(",").append(firstIP).append(",").append(firstPort).append(",")
    	                .append(secondIP).append(",").append(secondPort).append(" ");
            		} catch(Exception e){
            			logger.error(e.getMessage());
            		}	
            	}
        		
        		metadata = sb.toString().trim();
            	logger.info("The metadata: " + metadata);
            	
    			try {
    				//writeLock the deleted server
    				connect(ipAddr, Integer.valueOf(portNum), "ecs lockwrite");
    				//writeLock target server
    				connect(targetIp, Integer.valueOf(targetPort), "ecs lockwrite");
    				
    				logger.info("Both servers locked");
    				
    				//send all data from the server to be deleted to the target server
    				logger.info("Moving data from deleted server to target server");
    				String command = "ecs movedata " + startHash + "-" + endHash + " " + targetIp + "-" + targetPort;
    				connect(ipAddr, Integer.valueOf(portNum), command);
    				
    				//update metadata of all servers
    				updateAllServers(metadata);
    				logger.info("Metadata updated for all servers");
    				
    				//unlock write permission for the deleted server
    				connect(ipAddr, Integer.valueOf(portNum), "ecs unlockwrite");
    				
    				//unlock write permission for the target server
    				connect(targetIp, Integer.valueOf(targetPort), "ecs unlockwrite");
    				
    				logger.info("Write permission given");
    				
    				//add server to non participating list, and shut it down
    				notParticipating.add(ipAddr + " " + portNum);
    				connect(ipAddr, Integer.valueOf(portNum), "ecs shutdown");
    				
    				//write lock all servers
    				logger.info("All servers being write locked to initiate replication");
    				writeLockAll();
    				
    				//initiate replication in all servers
    				logger.info("Replicate all data in every server due to new added node");
    				initiateReplication();
    				
    				//allow writing of data again in all servers
    				logger.info("All servers are being given write permission - replication complete");
    				writeUnlockAll();
    				
    				logger.info("Number of participating servers after deletion: " + participatingServers.size());
    			} catch (Exception e) {
    				logger.error(e.getMessage());
    			}
    		} else {
    			//there are only three servers left, tell user to use shutdown instead
    			printError("A minimum of three servers must" +
    					" run at a time, use \"shutdown\" instead to perform a complete shutdown");
    		}
    	} else {
    		printError("The server requested is not running");
    	}
    }

    public void initService(int numberOfNodes, int cacheSize, String replacementStrategy){
        
    	if (numberOfNodes <= mIpAndPorts.size() && numberOfNodes >= 3){
	    	//randomly pick a group of nodes
	    	List<String> temp = new ArrayList<String>(mIpAndPorts);
	    	List<String> param = new ArrayList<String>();
	    	
	    	while(numberOfNodes != 0){
	    		Random rand = new Random();
	    		int index = rand.nextInt(temp.size());
	    		
	    		param.add(temp.get(index));
	    		temp.remove(index);
	    		numberOfNodes--;
	    	}
	    	
	    	notParticipating = temp;
	    	
	    	logger.info("Setting up Hasher");
	        cs = new ConsistentHashing(1,param);
	        
	        StringBuilder sb = new StringBuilder("");
	        participatingServers = new ArrayList<String>();
	        
	        numberOfNodes = param.size();
        
        	logger.info("Adding the particapating servers and setting up the metadata to be sent");
        	
        	for (int i=0; i<numberOfNodes; i++){
        		participatingServers.add(param.get(i));
                
        		try{
        			String[] elements= param.get(i).split(" ");
	                String ip = elements[0];
	                String port =elements[1];
	
	                ConsistentHashing.HashedServer hashedServer= cs.get(param.get(i));
	                String start = hashedServer.mHashedKeys[0].trim();
	                String end = hashedServer.mHashedKeys[1].trim();
	                String[] replicaAddr = cs.getReplicas(end);
	                
	                String[] firstIpPort = replicaAddr[0].split(" ");
	                String firstIP = firstIpPort[0];
	                String firstPort = firstIpPort[1];
	                
	                String[] secondIpPort = replicaAddr[1].split(" ");
	                String secondIP = secondIpPort[0];
	                String secondPort = secondIpPort[1];
	                
	                //FORMAT: startingHash,endingHash,ip,port,firstIP,firstPort,secondIP,secondPort 
	                sb.append(start.trim()).append(",").append(end.trim()).append(",")
	                .append(ip.trim()).append(",").append(port.trim())
	                .append(",").append(firstIP).append(",").append(firstPort).append(",")
	                .append(secondIP).append(",").append(secondPort).append(" ");
        		} catch(Exception e){
        			logger.error(e.getMessage());
        		}
        		
        	}
        	
        	metadata = sb.toString().trim();
        	logger.info("The metadata: " + metadata);
        	logger.info("Sending metadata and initializing all servers");
        	
            for (int i=0; i<numberOfNodes; i++) {
				try {
					String[] elements= param.get(i).split(" ");
	                String ip = elements[0];
	                String port =elements[1];
	                logger.info("ip passed in: "+ip+ " port: "+port);
					
					ssh(ip,port,"ecs initkvserver " + cacheSize + " " + replacementStrategy + " " + metadata);
					//connect(ip,50028,"ecs initkvserver " + cacheSize + " " + replacementStrategy + " " + metadata);
					
					ConsistentHashing.HashedServer hashedServer= cs.get(param.get(i));
	                String start = hashedServer.mHashedKeys[0].trim();
	                String end = hashedServer.mHashedKeys[1].trim();
					
					//send persisted data to server that should have it
	                String kvData = "";
	                
	                for(String kvPair : kvList){
	                	//check if within hashRange
	                	if(checkIfInRange(kvPair.split(",")[0], start, end)){
	                		String key = kvPair.split(",", 2)[0];
	                		String value = kvPair.split(",", 2)[1].replaceAll(" ", "-");;
	                		kvData = kvData + key + "," + value + " ";
	                	}
	                }
	                
	                if(!kvData.equals("")){
	                	logger.info("Sending persisted data");
	                	logger.info(kvData.trim());
						connect(ip,Integer.parseInt(port),"ecs addkvpairs " + kvData.trim());
	                } else {
	                	logger.info("No persistent data to send");
	                }
	                
				} catch (Exception e) {
					logger.error(e.getMessage());
				}
				
            }
            
            hbThreadList = new ArrayList<HeartBeatThread>();
            
            logger.info("Starting failure detection threads");
			for (int i = 0; i < participatingServers.size(); i++) {
				logger.info("Starting HeartBeat thread for: " + participatingServers.get(i));
				HeartBeatThread hbThread = new HeartBeatThread(participatingServers.get(i));
				hbThreadList.add(hbThread);
				hbThread.start();
			}
            
            initiateReplication();
        }else{
        	if(numberOfNodes >= 3){
        		printError("Number of nodes entered is larger than ones available can only enter at most " + mIpAndPorts.size());
        	} else {
        		printError("Number of nodes entered is less than 3, at least 3 servers are required at startup");
        	}  
        }
    }
    
    public void initiateReplication(){
    	//initiate replication in each server (sending of data to replicas)
        logger.info("Sending replication command to each server");
        for(String server : participatingServers){
        	String[] ipPort = server.split(" ");
        	
        	try{
        		connect(ipPort[0], Integer.parseInt(ipPort[1]), "ecs replicate");
        	} catch (Exception e){
        		logger.error(e.getMessage());
        	}
        }
    }
    
    public boolean checkIfInRange(String key, String start, String end) {
		String keyHash = cs.hashFunction(key);
		
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

    public String connect(String kvAddress, int kvPort, String metadata) throws Exception {
        boolean connected = false;
        String result = null;
        
        while(!connected){
	    	try{
	            OutputStream output;
	            InputStream input;
	            //System.out.println("KVADDRESS: "+kvAddress+" PORT: "+kvPort);
	            Socket clientSocket = new Socket();
	            clientSocket.connect(new InetSocketAddress(kvAddress, kvPort));
	            
	            logger.info("CONNECTED TO SERVER");
	            
	            connected = true;
	            
	            output = clientSocket.getOutputStream();
	            input = clientSocket.getInputStream();
            	
	            //send message to server
	            sendMessage(new TextMessage(metadata), output);
	            
	            result = receiveMessage(input).getMsg();
	            
	            if(result.split(" ").length > 1){
	            	System.out.println("Server with IP " + kvAddress + " and PORT " + kvPort + " replies: " + result.split(" ", 2)[0]);
	            	result = result.split(" ", 2)[1];
	            } else {
	            	System.out.println("Server with IP " + kvAddress + " and PORT " + kvPort + " replies: " + result);
	            	result = "";
	            }
	
	            disconnect(clientSocket,input,output,kvAddress,Integer.toString(kvPort));
	
	        }catch (Exception e){
	        	result = null;
	        }
        }
        
        return result;
    }

    public void disconnect(Socket clientSocket, InputStream input, OutputStream output, String kvAddress, String kvPort) {
        
    	logger.info("Disconnecting " + kvAddress + ":" + kvPort);
        if(clientSocket != null){
            try{
                clientSocket.close();
                input.close();
                output.close();
            }catch (IOException e){
                logger.error(e.getMessage());
            }

            clientSocket = null;
            logger.info("Connection to " + kvAddress + ":" + kvPort + " closed");
        }
    }

    /**
     * Method sends a TextMessage using this socket.
     * @param msg the message that is to be sent.
     * @throws IOException some I/O error regarding the output stream
     */
    public void sendMessage(TextMessage msg, OutputStream output) throws IOException {
        byte[] msgBytes = msg.getMsgBytes();
        output.write(msgBytes, 0, msgBytes.length);
        output.flush();
    }

    public TextMessage receiveMessage(InputStream input) throws IOException {


        int index = 0;
        byte[] msgBytes = null, tmp = null;
        byte[] bufferBytes = new byte[BUFFER_SIZE];

		/* read first char from stream */
        byte read = (byte) input.read();
        boolean reading = true;

        while(read != 13 && reading) {/* carriage return */
			/* if buffer filled, copy to msg array */
            if(index == BUFFER_SIZE) {
                if(msgBytes == null){
                    tmp = new byte[BUFFER_SIZE];
                    System.arraycopy(bufferBytes, 0, tmp, 0, BUFFER_SIZE);
                } else {
                    tmp = new byte[msgBytes.length + BUFFER_SIZE];
                    System.arraycopy(msgBytes, 0, tmp, 0, msgBytes.length);
                    System.arraycopy(bufferBytes, 0, tmp, msgBytes.length,
                            BUFFER_SIZE);
                }

                msgBytes = tmp;
                bufferBytes = new byte[BUFFER_SIZE];
                index = 0;
            }

			/* only read valid characters, i.e. letters and numbers */
            if((read > 31 && read < 127)) {
                bufferBytes[index] = read;
                index++;
            }

			/* stop reading is DROP_SIZE is reached */
            if(msgBytes != null && msgBytes.length + index >= DROP_SIZE) {
                reading = false;
            }

			/* read next char from stream */
            read = (byte) input.read();
        }

        if(msgBytes == null){
            tmp = new byte[index];
            System.arraycopy(bufferBytes, 0, tmp, 0, index);
        } else {
            tmp = new byte[msgBytes.length + index];
            System.arraycopy(msgBytes, 0, tmp, 0, msgBytes.length);
            System.arraycopy(bufferBytes, 0, tmp, msgBytes.length, index);
        }

        msgBytes = tmp;

		/* build final String */
        TextMessage msg = new TextMessage(msgBytes);
//        System.out.print("Receive message:\t '" + msg.getMsg() + "'");

        return msg;
    }


    public void ssh(String ip, String port, String metadata){

        Process proc = null;
        String script = "src/app_kvEcs/script.sh";
        String[] cmd = {"sh", script, ip, port};

        Runtime run = 	Runtime.getRuntime();
        try {
            proc = run.exec(cmd);
            
            //wait till the server notifies that it is about to listen for connections, then proceed
            ServerSocket ss = new ServerSocket(ecsPort);
            ss.accept();
            
            ss.close();
            
        } catch (IOException e) {
            logger.error(e.getMessage());
        }finally {
            if (proc!=null){
                logger.info("killing process!");
                proc.destroy();

                try {
                    logger.info("SSH: "+ip+" PORT: "+ port);
                    connect(ip, Integer.parseInt(port), metadata);

                } catch (Exception e) {
                	logger.error(e.getMessage());
                }
            }
        }
    }
    private void printError(String error){
        System.out.println("ERROR! " +  error);
    }

    public void readFile() {

        BufferedReader br = null;
        FileReader fr = null;
        String FILENAME = "src/app_kvEcs/ecs.conf";
        mIpAndPorts = new ArrayList<>();

        try {

            fr = new FileReader(FILENAME);
            br = new BufferedReader(fr);

            String sCurrentLine;

            br = new BufferedReader(new FileReader(FILENAME));

            while ((sCurrentLine = br.readLine()) != null) {
                System.out.println(sCurrentLine);
                String[] splitString = sCurrentLine.split(" ");
                String ipAddress = splitString[1].trim();
                String port = splitString[2].trim();
                mIpAndPorts.add(ipAddress+" "+port);
            }

        } catch (IOException e) {

            logger.error(e.getMessage());

        } finally {

            try {

                if (br != null)
                    br.close();

                if (fr != null)
                    fr.close();

            } catch (IOException ex) {

            	logger.error(ex.getMessage());

            }

        }
    }

    //--------------------------------------------------------------------------------HEARTBEAT THREAD CODE STARTS----------------------------------------------//
    private class HeartBeatThread extends Thread {

    	// store server information for which heartbeat is running
		String mServerInfo; 
		boolean failureDetected = false;
		
		public HeartBeatThread(String serverInfo) {
			mServerInfo = serverInfo;
		}

		@Override
		public void run() {
			super.run();
			//run a version of connect
			//send message and then recieve message in a loop
			//catch the exception and add timeout
			//when error caught call the recovery function
			String[] infoArray = mServerInfo.split(" ");
			String ipAddress = infoArray[0];
			String portNumber = infoArray[1];
			int port = Integer.valueOf(portNumber);
			logger.info("HeartBeat thread ---- Calling hbConnect ---- " + ipAddress + " " + portNumber);
			hbConnect(ipAddress, port);
		}

		private void hbConnect(String kvAddress, int kvPort) {

			while (!failureDetected){
				try {
					String result;
					
					//check if server is responsive every 5 seconds
					connectHeartBeat(kvAddress, kvPort, "ecs heartbeat");
					
					Thread.sleep(5000);

				}
				catch (Exception e) {

					failureDetected = true;
					failureRecovery(kvAddress + " " + kvPort);
				}
			}
		}

		/**
		 * Threads are accessing global variables here:
		 * 1. participating servers
		 * 2. the consistent hashing circle
		 *
		 * @param serverHash
		 */
		private synchronized void failureRecovery(String serverHash) {
			logger.error("FAILURE HAS OCCURRED OF SERVER: " + serverHash);
			pauseOnFailure = true;
			
			String metadata = computeMetadata(participatingServers);
			logger.info("Metadata on entering Failure Recovery:  "+ metadata);

			//1.remove the server and move appropriate data
			int index = mIpAndPorts.indexOf(serverHash);
			deleteNodeForFailure(index + 1);
			
			//2.call addNode -- already sends the most updated metadata to all the servers
			addNode(10,"fifo");

			String updatedmetadata = computeMetadata(participatingServers);
			//for debugging purposes only
			logger.info("METADATA TO SEND  ----------------------------------------------------------------------------  ");
			logger.info(updatedmetadata);

			logger.error("Failure Recovery now complete --- Resume operation");
			pauseOnFailure = false;
		}
		
		private void deleteNodeForFailure(int servNum){
			logger.info("Number of participating servers: " + participatingServers.size());
	    	if(participatingServers.contains(mIpAndPorts.get(servNum - 1))){
	    		if(participatingServers.size() > 3){
	    			//get required information to move data allocated to an existing server
	        		String[] ipAndport = mIpAndPorts.get(servNum - 1).split(" ");
	                String ipAddr = ipAndport[0];
	                String portNum = ipAndport[1];

	                ConsistentHashing.HashedServer hS = cs.get(mIpAndPorts.get(servNum - 1));
	                String startHash = hS.mHashedKeys[0].trim();
	                String endHash = hS.mHashedKeys[1].trim();
	                
	                //we will send the moveData request to this IP and Port server
	                String targetIp = null;
	                String targetPort = null;
	                
	                //remove the server from participatingServers and the HashCircle and the heartbeat thread
	                int remIndex = participatingServers.indexOf(ipAddr + " " + portNum);
	                participatingServers.remove(remIndex);
	                
	                HeartBeatThread hb = hbThreadList.get(remIndex);
	                hb.failureDetected = true;
	                hbThreadList.remove(remIndex);
	                
	                cs.remove(ipAddr + " " + portNum);
	                
	                //update metadata and find server that is right after deleted server on circle
	        		logger.info("Updating metadata without the deleted server");
	        		StringBuilder sb = new StringBuilder("");
	        		
	        		for (String ipAndPort: participatingServers){
	                    
	            		try{
	            			String[] elements= ipAndPort.split(" ");
	    	                String ip = elements[0];
	    	                String port =elements[1];
	    	
	    	                ConsistentHashing.HashedServer hashedServer= cs.get(ipAndPort);
	    	                String start = hashedServer.mHashedKeys[0].trim();
	    	                String end = hashedServer.mHashedKeys[1].trim();
	    	                String[] replicaAddr = cs.getReplicas(end);
	    	                
	    	                String[] firstIpPort = replicaAddr[0].split(" ");
	    	                String firstIP = firstIpPort[0];
	    	                String firstPort = firstIpPort[1];
	    	                
	    	                String[] secondIpPort = replicaAddr[1].split(" ");
	    	                String secondIP = secondIpPort[0];
	    	                String secondPort = secondIpPort[1];
	    	                
	    	                if(start.equals(startHash)){
	    	                	targetIp = ip;
	    	                	targetPort = port;
	    	                	logger.info("Found target server");
	    	                }
	    	                
	    	                //FORMAT: startingHash,endingHash,ip,port,firstIP,firstPort,secondIP,secondPort 
	    	                sb.append(start.trim()).append(",").append(end.trim()).append(",")
	    	                .append(ip.trim()).append(",").append(port.trim())
	    	                .append(",").append(firstIP).append(",").append(firstPort).append(",")
	    	                .append(secondIP).append(",").append(secondPort).append(" ");
	            		} catch(Exception e){
	            			logger.error(e.getMessage());
	            		}	
	            	}
	        		
	        		metadata = sb.toString().trim();
	            	logger.info("The metadata: " + metadata);
	            	
	            	//since deleted server has crashed, just tell target server to move data from firstReplica file to its main storage file
	    			try {
	    				//writeLock target server
	    				connect(targetIp, Integer.valueOf(targetPort), "ecs lockwrite");
	    				
	    				logger.info("Target server locked");
	    				
	    				//send all data from the server to be deleted to the target server
	    				logger.info("Moving data from firstReplica file to main storage file");
	    				String command = "ecs firstreplicadead";
	    				connect(targetIp, Integer.valueOf(targetPort), command);
	    				
	    				//update metadata of all servers
	    				updateAllServers(metadata);
	    				logger.info("Metadata updated for all servers");
	    				
	    				//unlock write permission for the target server
	    				connect(targetIp, Integer.valueOf(targetPort), "ecs unlockwrite");
	    				
	    				logger.info("Write permission given");
	    				
	    				//add server to non participating list
	    				notParticipating.add(ipAddr + " " + portNum);
	    				
	    				//write lock all servers
	    				logger.info("All servers being write locked to initiate replication");
	    				writeLockAll();
	    				
	    				//initiate replication in all servers
	    				logger.info("Replicate all data in every server due to new added node");
	    				initiateReplication();
	    				
	    				//allow writing of data again in all servers
	    				logger.info("All servers are being given write permission - replication complete");
	    				writeUnlockAll();
	    				
	    				logger.info("Number of participating servers after deletion: " + participatingServers.size());
	    			} catch (Exception e) {
	    				logger.error(e.getMessage());
	    			}
	    		} else {
	    			//there are only three servers left, tell user to use shutdown instead
	    			printError("Only three servers left, delete not possible");
	    		}
	    	} else {
	    		printError("The server requested is not running");
	    	}
		}
	}
    
    public String connectHeartBeat(String kvAddress, int kvPort, String metadata) throws Exception {
        String result = null;
        
        OutputStream output;
        InputStream input;
        //System.out.println("KVADDRESS: "+kvAddress+" PORT: "+kvPort);
        Socket clientSocket = new Socket();
        clientSocket.connect(new InetSocketAddress(kvAddress, kvPort));
        
        logger.info("CONNECTED TO SERVER");
        
        output = clientSocket.getOutputStream();
        input = clientSocket.getInputStream();
    	
        //send message to server
        sendMessage(new TextMessage(metadata), output);
        
        result = receiveMessage(input).getMsg();
        
//        if(result.split(" ").length > 1){
//        	System.out.println("Server with IP " + kvAddress + " and PORT " + kvPort + " replies: " + result.split(" ", 2)[0]);
//        	result = result.split(" ", 2)[1];
//        } else {
//        	System.out.println("Server with IP " + kvAddress + " and PORT " + kvPort + " replies: " + result);
//        	result = "";
//        }

        disconnect(clientSocket,input,output,kvAddress,Integer.toString(kvPort));
        
        return result;
    }
    
    public String computeMetadata(List<String> param) {
		String metadata;
		StringBuilder sb = new StringBuilder("");
		
		for (String ipAndPort: participatingServers){
            
    		try{
    			String[] elements= ipAndPort.split(" ");
                String ip = elements[0];
                String port =elements[1];

                ConsistentHashing.HashedServer hashedServer= cs.get(ipAndPort);
                String start = hashedServer.mHashedKeys[0].trim();
                String end = hashedServer.mHashedKeys[1].trim();
                String[] replicaAddr = cs.getReplicas(end);
                
                String[] firstIpPort = replicaAddr[0].split(" ");
                String firstIP = firstIpPort[0];
                String firstPort = firstIpPort[1];
                
                String[] secondIpPort = replicaAddr[1].split(" ");
                String secondIP = secondIpPort[0];
                String secondPort = secondIpPort[1];
                
                //FORMAT: startingHash,endingHash,ip,port,firstIP,firstPort,secondIP,secondPort 
                sb.append(start.trim()).append(",").append(end.trim()).append(",")
                .append(ip.trim()).append(",").append(port.trim())
                .append(",").append(firstIP).append(",").append(firstPort).append(",")
                .append(secondIP).append(",").append(secondPort).append(" ");
    		} catch(Exception e){
    			logger.error(e.getMessage());
    		}	
    	}
		
		metadata = sb.toString().trim();
		return metadata;
	}
    
public void initServiceTest(int numberOfNodes, int cacheSize, String replacementStrategy){
        
    	if (numberOfNodes <= mIpAndPorts.size() && numberOfNodes >= 3){
	    	//randomly pick a group of nodes
	    	List<String> temp = new ArrayList<String>(mIpAndPorts);
	    	List<String> param = new ArrayList<String>();
	    	
	    	while(numberOfNodes != 0){
	    		Random rand = new Random();
	    		int index = rand.nextInt(temp.size());
	    		
	    		param.add(temp.get(index));
	    		temp.remove(index);
	    		numberOfNodes--;
	    	}
	    	
	    	notParticipating = temp;
	    	
	    	logger.info("Setting up Hasher");
	        cs = new ConsistentHashing(1,param);
	        
	        StringBuilder sb = new StringBuilder("");
	        participatingServers = new ArrayList<String>();
	        
	        numberOfNodes = param.size();
        
        	logger.info("Adding the particapating servers and setting up the metadata to be sent");
        	
        	for (int i=0; i<numberOfNodes; i++){
        		participatingServers.add(param.get(i));
                
        		try{
        			String[] elements= param.get(i).split(" ");
	                String ip = elements[0];
	                String port =elements[1];
	
	                ConsistentHashing.HashedServer hashedServer= cs.get(param.get(i));
	                String start = hashedServer.mHashedKeys[0].trim();
	                String end = hashedServer.mHashedKeys[1].trim();
	                String[] replicaAddr = cs.getReplicas(end);
	                
	                String[] firstIpPort = replicaAddr[0].split(" ");
	                String firstIP = firstIpPort[0];
	                String firstPort = firstIpPort[1];
	                
	                String[] secondIpPort = replicaAddr[1].split(" ");
	                String secondIP = secondIpPort[0];
	                String secondPort = secondIpPort[1];
	                
	                //FORMAT: startingHash,endingHash,ip,port,firstIP,firstPort,secondIP,secondPort 
	                sb.append(start.trim()).append(",").append(end.trim()).append(",")
	                .append(ip.trim()).append(",").append(port.trim())
	                .append(",").append(firstIP).append(",").append(firstPort).append(",")
	                .append(secondIP).append(",").append(secondPort).append(" ");
        		} catch(Exception e){
        			logger.error(e.getMessage());
        		}
        		
        	}
        	
        	metadata = sb.toString().trim();
        	logger.info("The metadata: " + metadata);
        	logger.info("Sending metadata and initializing all servers");
        	
            for (int i=0; i<numberOfNodes; i++) {
				try {
					String[] elements= param.get(i).split(" ");
	                String ip = elements[0];
	                String port =elements[1];
	                logger.info("ip passed in: "+ip+ " port: "+port);
					
					ssh(ip,port,"ecs initkvserver " + cacheSize + " " + replacementStrategy + " " + metadata);
					//connect(ip,50028,"ecs initkvserver " + cacheSize + " " + replacementStrategy + " " + metadata);
					
					ConsistentHashing.HashedServer hashedServer= cs.get(param.get(i));
	                String start = hashedServer.mHashedKeys[0].trim();
	                String end = hashedServer.mHashedKeys[1].trim();
					
					//send persisted data to server that should have it
	                String kvData = "";
	                
	                for(String kvPair : kvList){
	                	//check if within hashRange
	                	if(checkIfInRange(kvPair.split(",")[0], start, end)){
	                		String key = kvPair.split(",", 2)[0];
	                		String value = kvPair.split(",", 2)[1].replaceAll(" ", "-");;
	                		kvData = kvData + key + "," + value + " ";
	                	}
	                }
	                
	                if(!kvData.equals("")){
	                	logger.info("Sending persisted data");
	                	logger.info(kvData.trim());
						connect(ip,Integer.parseInt(port),"ecs addkvpairs " + kvData.trim());
	                } else {
	                	logger.info("No persistent data to send");
	                }
	                
				} catch (Exception e) {
					logger.error(e.getMessage());
				}
				
            }
            
//            hbThreadList = new ArrayList<HeartBeatThread>();
//            
//            logger.info("Starting failure detection threads");
//			for (int i = 0; i < participatingServers.size(); i++) {
//				logger.info("Starting HeartBeat thread for: " + participatingServers.get(i));
//				HeartBeatThread hbThread = new HeartBeatThread(participatingServers.get(i));
//				hbThreadList.add(hbThread);
//				hbThread.start();
//			}
            
            initiateReplication();
        }else{
        	if(numberOfNodes >= 3){
        		printError("Number of nodes entered is larger than ones available can only enter at most " + mIpAndPorts.size());
        	} else {
        		printError("Number of nodes entered is less than 3, at least 3 servers are required at startup");
        	}  
        }
    }
}


