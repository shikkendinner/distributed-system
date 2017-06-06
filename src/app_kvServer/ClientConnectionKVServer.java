package app_kvServer;

import common.messages.KVMessage;
import common.messages.KVAdminMessage;
import org.apache.log4j.Logger;

import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.net.Socket;


/**
 * Represents a connection end point for a particular client that is 
 * connected to the server. This class is responsible for message reception 
 * and sending. 
 * The class also implements the echo functionality. Thus whenever a message 
 * is received it is going to be echoed back to the client.
 */
public class ClientConnectionKVServer implements Runnable {

	private static Logger logger = Logger.getRootLogger();
	
	private boolean isOpen;
	private static final int BUFFER_SIZE = 1024;
	private static final int DROP_SIZE = 128 * BUFFER_SIZE;

	public static final int ECSport = 40000;
	
	private Socket clientSocket;
	private InputStream input;
	private OutputStream output;
	public KVServer kvServerListener;
	public static final String FORMAT_ERROR = "Error: The format you entered is incorrect. Type 'help' to see possible options";
	
	private boolean shutdown;
	
	/**
	 * Constructs a new CientConnection object for a given TCP socket.
	 * @param clientSocket the Socket object for the client connection.
	 */
	public ClientConnectionKVServer(Socket clientSocket) {
		this.clientSocket = clientSocket;
		this.isOpen = true;
		shutdown = false;
	}

	/**
	 * set the listener to tell te KVServer the parsed message
	 * @param kvServerListener
	 */
	public void setKVServerListener(KVServer kvServerListener){
		this.kvServerListener = kvServerListener;
	}

	/**
	 * Initializes and starts the client connection. 
	 * Loops until the connection is closed or aborted by the client.
	 */
	public void run() {
		try {
			output = clientSocket.getOutputStream();
			input = clientSocket.getInputStream();
		
			logger.info("Connection to KV Server established: " 
					+ clientSocket.getLocalAddress() + " / "
					+ clientSocket.getLocalPort());
			
			while(isOpen) {
				try {
					TextMessageKVServer latestMsg = receiveMessage();
					sendMessage(latestMsg);
					
				/* connection either terminated by the client or lost due to 
				 * network problems*/	
				} catch (IOException ioe) {
					logger.error("Error! Connection lost!");
					isOpen = false;
				}				
			}
			
		} catch (IOException ioe) {
			logger.error("Error! Connection could not be established!", ioe);
			
		} finally {
			disconnect();
		}
	}

	/**
	 * Disconnect the server and close sockets properly
	 */
	public void disconnect(){
		try {
			if (clientSocket != null) {
				input.close();
				output.close();
				clientSocket.close();
			}
			if(shutdown){
				System.exit(1);
			}
		} catch (IOException ioe) {
			logger.error("Error! Unable to tear down connection!", ioe);
		}
	}
	/**
	 * Method sends a TextMessage using this socket.
	 * @param msg the message that is to be sent.
	 * @throws IOException some I/O error regarding the output stream 
	 */
	public void sendMessage(TextMessageKVServer msg) throws IOException {
		byte[] msgBytes = msg.getMsgBytes();
		output.write(msgBytes, 0, msgBytes.length);
		output.flush();
		logger.info("SEND \t<" 
				+ clientSocket.getInetAddress().getHostAddress() + ":" 
				+ clientSocket.getPort() + ">: '" 
				+ msg.getMsg() +"'");
    }
	
	
	private TextMessageKVServer receiveMessage() throws IOException {
		
		int index = 0;
		byte[] msgBytes = null, tmp = null;
		byte[] bufferBytes = new byte[BUFFER_SIZE];
		
		/* read first char from stream */
		byte read = (byte) input.read();	
		boolean reading = true;
		
//		logger.info("First Char: " + read);
//		Check if stream is closed (read returns -1)
//		if (read == -1){
//			TextMessage msg = new TextMessage("");
//			return msg;
//		}

		while(/*read != 13  && */ read != 10 && read !=-1 && reading) {/* CR, LF, error */
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
			
			/* only read valid characters, i.e. letters and constants */
			bufferBytes[index] = read;
			index++;
			
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
		TextMessageKVServer msg = new TextMessageKVServer(msgBytes);

		TextMessageKVServer response = new TextMessageKVServer(processMessage(msg.getMsg()).getBytes());

		return response;

    }

    public String processMessage(String message){
		String[] messageArray = message.split(" ");
		String status = FORMAT_ERROR;

		String action = messageArray[0].trim();
		logger.info("ACTION: " + action);

		//Check if it is the ECS Server, or a client
		if(action.equals("ecs")){
			//the request came from the ECS server
			//check which method is being invoked by the ECS
			action = messageArray[1].trim();
			if(action.equals(KVServer.INIT)){
				String[] mParams = message.split(" ", 5);
				KVAdminMessage kvAM = kvServerListener.initKVServer(mParams[4].trim().split(" "), Integer.parseInt(mParams[2].trim()), mParams[3].trim());
				status = String.valueOf(kvAM.getStatus());
			} else if(action.equals(KVServer.START)){
				//start() method being invoked in the KVServer
				KVAdminMessage kvAM = kvServerListener.start();
				status = String.valueOf(kvAM.getStatus());
			} else if(action.equals(KVServer.STOP)){
				//stop() method being invoked in the KVServer
				KVAdminMessage kvAM = kvServerListener.stop();
				status = String.valueOf(kvAM.getStatus());
			} else if(action.equals(KVServer.SHUTDOWN)){
				//shutting down the server, this connection will be broken
				KVAdminMessage kvAM = kvServerListener.shutDown();
				isOpen = false;
				shutdown = true;
				status = String.valueOf(kvAM.getStatus()) + " " + kvAM.getData();
			} else if(action.equals(KVServer.LOCKWRITE)){
				//setting banWrite global variable to lock put operations
				KVAdminMessage kvAM = kvServerListener.lockWrite();
				status = String.valueOf(kvAM.getStatus());
			} else if(action.equals(KVServer.UNLOCKWRITE)){
				//set the banWrite global variable to allow put operations
				KVAdminMessage kvAM = kvServerListener.unLockWrite();
				status = String.valueOf(kvAM.getStatus());
			} else if(action.equals(KVServer.MOVEDATA)){
				// move the data from this server to the provided server
				KVAdminMessage kvAM = kvServerListener.moveData(messageArray[2], messageArray[3]);
				status = String.valueOf(kvAM.getStatus()) + " " + kvAM.getData();
			} else if(action.equals(KVServer.UPDATE)){
				//update the metadata

				String[] mParams = message.split(" ", 3);

				KVAdminMessage kvAM = kvServerListener.update(mParams[2].split(" "));
				status = String.valueOf(kvAM.getStatus());
			} else if(action.equals(KVServer.DELETEPAIRS)){
				//delete the key-value pairs the ECF has sent
				String[] kvList = message.split(" ", 3)[2].split(" ");
				
				KVAdminMessage kvAM = kvServerListener.deletePairs(kvList);
				status = String.valueOf(kvAM.getStatus());
			} else if(action.equals(KVServer.ADDKV)){
				//send the data to the KVServer method
				status = kvServerListener.addKVPairs(message.split(" ", 3)[2].trim().split(" "));
			} else if(action.equals(KVServer.REPLICATE)){
				//run the replicate command
				KVAdminMessage kvAM = kvServerListener.replicate();
				status = String.valueOf(kvAM.getStatus());
			} else if(action.equals(KVServer.FIRSTREPLICADEAD)){
				//run the firstReplicaDead command
				KVAdminMessage kvAM = kvServerListener.firstReplicaDead();
				status = String.valueOf(kvAM.getStatus());
			} else if(action.equals(KVServer.HEARTBEAT)){
				//run the firstReplicaDead command
				KVAdminMessage kvAM = new KVAdminMessageStorage(KVAdminMessage.StatusType.HEARTBEAT_RECEIVED, "");
				status = String.valueOf(kvAM.getStatus());
			} else if(action.equals(KVServer.FAIL)){
				//Shutdown server unexpectedly
				System.exit(1);
			}

			return status;
		} else if(action.equals("server")){
			//the request came from a data server
			//check which method is being invoked by that server
			action = messageArray[1].trim();
			
			if(action.equals("addkvpairs")){
				//send the data to the KVServer method
				status = kvServerListener.addKVPairs(message.split(" ", 3)[2].split(" "));
			} else if(action.equals("addreplicadata")){
				String[] splitMsg = message.trim().split(" ", 4);
				
				if(splitMsg.length == 3){
					String[] temp = {""};
					status = kvServerListener.addReplicaData(Integer.parseInt(splitMsg[2].trim()), temp);
				} else{
					status = kvServerListener.addReplicaData(Integer.parseInt(splitMsg[2].trim()), splitMsg[3].split(" "));
				}
			} else if(action.equals("putreplica")){
				String[] splitMsg = message.trim().split(" ", 5);
				
				status = kvServerListener.putReplica(Integer.parseInt(splitMsg[2]), splitMsg[3].trim(), splitMsg[4].trim());
			}
			
			return status;
		} else {
			if (messageArray.length>=2){
				try {
					//determining the action type based on the length of the message
					//this is under the assumption that the key will not have spaces
					String key= messageArray[1].trim();

					if (messageArray.length>2 && action.equals(KVServer.PUT)){

						if(kvServerListener.state == KVServer.SERVER_READY){
							if(kvServerListener.banWrite == KVServer.ALLOW_WRITE){
								StringBuilder sb = new StringBuilder("");
								for (int i = 2; i< messageArray.length; i++) {

									sb.append(messageArray[i]).append(" ");
								}

								KVMessage kvMessage = kvServerListener.put(key, sb.toString().trim() );
								if(String.valueOf(kvMessage.getStatus()).trim().equals("SERVER_NOT_RESPONSIBLE")){
									status = String.valueOf(kvMessage.getStatus())+" "+kvMessage.getKey();
								} else{
									status = String.valueOf(kvMessage.getStatus())+" "+kvMessage.getKey() + " "+ kvMessage.getValue();
								} 
								
							} else {
								status = String.valueOf(KVMessage.StatusType.SERVER_WRITE_LOCK)+" null null";
							}
						} else {
							status = String.valueOf(KVMessage.StatusType.SERVER_STOPPED)+" null null";
						}

					}else if (messageArray.length==2){

						if(kvServerListener.state == KVServer.SERVER_READY){
							//must be a get
							if (action.equals(KVServer.GET)){
								KVMessage kvMessage = kvServerListener.get(key);

								if(String.valueOf(kvMessage.getStatus()).trim().equals("SERVER_NOT_RESPONSIBLE")){
									status = String.valueOf(kvMessage.getStatus())+" "+kvMessage.getKey();
								} else {
									status = String.valueOf(kvMessage.getStatus())+" "+kvMessage.getKey() + " "+ kvMessage.getValue();	
								}

							} else if(action.equals(KVServer.PUT)){
								if(kvServerListener.banWrite == KVServer.ALLOW_WRITE){
									KVMessage kvMessage = kvServerListener.put(key, null);
									status = String.valueOf(kvMessage.getStatus() + " " + kvMessage.getKey());
								} else {
									status = String.valueOf(KVMessage.StatusType.SERVER_WRITE_LOCK)+" null null";
								}
							}
						}
						else{
							status = String.valueOf(KVMessage.StatusType.SERVER_STOPPED)+" null null";
						}	
							
					}else if (action.equals(KVServer.SUBSCRIBE)){
						if(kvServerListener.state == KVServer.SERVER_READY){
							if(kvServerListener.banWrite == KVServer.ALLOW_WRITE){
								int port = Integer.parseInt(messageArray[2].trim());
								KVMessage kvMessage = kvServerListener.subscribe(key, port);
								
								if(String.valueOf(kvMessage.getStatus()).trim().equals("SERVER_NOT_RESPONSIBLE")){
									status = String.valueOf(kvMessage.getStatus())+" "+kvMessage.getKey();
								} else {
									if(String.valueOf(kvMessage.getStatus()).trim().equals("SUBSCRIBE_ERROR")){
										//SUBSCRIPTION FAILED SO REPLY WITH ERROR MESSAGE
										status = String.valueOf(kvMessage.getStatus())+" "+kvMessage.getKey();
									} else {
										//SUBSCRIPTION SUCCEEDED SO REPLY WITH KEY AND VALUE
										status = String.valueOf(kvMessage.getStatus())+" "+kvMessage.getKey()+" "+kvMessage.getValue();
									}
								}
							}else{
								status = String.valueOf(KVMessage.StatusType.SERVER_WRITE_LOCK)+" null null";
							}
						} else {
							status = String.valueOf(KVMessage.StatusType.SERVER_STOPPED)+" null null";
						}
					}else if (action.equals(KVServer.UNSUBSCRIBE)){
						if(kvServerListener.state == KVServer.SERVER_READY){
							if(kvServerListener.banWrite == KVServer.ALLOW_WRITE){
								int port = Integer.parseInt(messageArray[2].trim());
								KVMessage kvMessage = kvServerListener.unsubscribe(key, port);
								
								if(String.valueOf(kvMessage.getStatus()).trim().equals("SERVER_NOT_RESPONSIBLE")){
									status = String.valueOf(kvMessage.getStatus())+" "+kvMessage.getKey();
								} else {
									//UNSUBSCRIPTION SUCCEEDED OR FAILED HERE, key holds key or error message
									status = String.valueOf(kvMessage.getStatus())+" "+kvMessage.getKey();
								}
							}else{
								status = String.valueOf(KVMessage.StatusType.SERVER_WRITE_LOCK)+" null null";
							}
						} else {
							status = String.valueOf(KVMessage.StatusType.SERVER_STOPPED)+" null null";
						}
					}else{
	//					logger.error("message array length not greater than or equal to 2.");
						return FORMAT_ERROR;
					}


					/* 0 and 1 hold the action and the key*/
					logger.info("RECEIVE \t<"
							+ clientSocket.getInetAddress().getHostAddress() + ":"
							+ clientSocket.getPort() + ">: '"
							+ message.trim() + "'");
					//must be success here
					return status;
				} catch (Exception e) {
					logger.error(e.getMessage());
					return e.getMessage();
				}

			}else{
				return FORMAT_ERROR;
			}
		}
	}
}
