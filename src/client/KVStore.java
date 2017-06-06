package client;

import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.net.*;

import app_kvServer.KVMessageStorage;
import common.messages.KVMessage;

import javax.xml.soap.Text;
import org.apache.log4j.Logger;

public class KVStore implements KVCommInterface {

	private Socket clientSocket;
	private String kvAddress;
	private int kvPort;
	private OutputStream output;
	private InputStream input;
	private static Logger logger = Logger.getRootLogger();
	private static final int BUFFER_SIZE = 1024;
	private static final int DROP_SIZE = 1024*BUFFER_SIZE;
	private boolean connected;

	/**
	 * Initialize KVStore with address and port of KVServer
	 * @param address the address of the KVServer
	 * @param port the port of the KVServer
	 */
	public KVStore(String address, int port) {
		this.kvAddress = address;
		this.kvPort = port;
		this.clientSocket = null;
		input = null;
		output = null;
		connected = false;
	}

	public int getKvPort(){ return kvPort; }

	public String getKvAddress() {return kvAddress;}

	public boolean getConnected(){
		return connected;
	}

	public void setConnected(boolean c){
		connected = c;
	}
	
	@Override
	public void connect() throws Exception {
		// TODO Auto-generated method stub
		try{
			this.clientSocket = new Socket();
			this.clientSocket.connect(new InetSocketAddress(this.kvAddress,this.kvPort),1000);
			input = clientSocket.getInputStream();
			output = clientSocket.getOutputStream();
			setConnected(true);
			logger.info("socket established " + kvAddress + ":" + kvPort);
		//	TextMessage res = receiveMessage();
		}catch (SocketTimeoutException e){
			System.out.println("Connection failed, please try again");
			logger.info("time out when establishing socket " + kvAddress + ":" + kvPort);
			setConnected(false);
		}catch (ConnectException e){
			System.out.println("Connection failed, please try again");
			logger.info("connection is refused when establishing socket " + kvAddress + ":" + kvPort);
			setConnected(false);
		}
	}

	@Override
	public void disconnect() {
		// TODO Auto-generated method stub
		logger.info("disconnecting " + kvAddress + ":" + kvPort);
		if(clientSocket != null){
			try{
				setConnected(false);
				clientSocket.close();

			}catch (IOException e){
				e.printStackTrace();
			}

			clientSocket = null;
			logger.info("connection to " + kvAddress + ":" + kvPort + " closed");
		}
	}

	@Override
	public KVMessage put(String key, String value) throws Exception {
		// TODO Auto-generated method stub
		TextMessage msg = null;
		KVMessageStorage kvms = null;
		StringBuilder sb = new StringBuilder();

		sb.append("put ");
		if(key.getBytes().length <= 20){
			sb.append(key);
		}else{
			logger.info("key exceeds max length 20 Bytes");
			System.out.println("key exceeds max length 20 bytes");
			kvms = new KVMessageStorage(null, null, StatusTypeLookup("PUT_ERROR"));
			return kvms;
		}

		if(value.equals("null")){
			logger.info("trying to delete key " + key);
			System.out.println("Deleting key " + key.trim());
			msg = new TextMessage(sb.toString());
			sendMessage(new TextMessage(sb.toString()));
			TextMessage res = receiveMessage();
			String[] tokens = res.getMsg().split("\\s+",2);
			System.out.println("KEY: " + key.trim());
			System.out.println("VALUE: " + value.trim());
			System.out.println("STATUS: " + tokens[0].trim());
			
			if(StatusTypeLookup(tokens[0]) == KVMessage.StatusType.SERVER_NOT_RESPONSIBLE){
				kvms = new KVMessageStorage(null,tokens[1],StatusTypeLookup("SERVER_NOT_RESPONSIBLE"));
			}else if (StatusTypeLookup(tokens[0]) == KVMessage.StatusType.SERVER_STOPPED){
				kvms = new KVMessageStorage(null,null, StatusTypeLookup("SERVER_STOPPED"));
				System.out.println("Operation is not successful, please try again later.");
			}else if (StatusTypeLookup(tokens[0]) == KVMessage.StatusType.SERVER_WRITE_LOCK){
				kvms = new KVMessageStorage(null,null, StatusTypeLookup("SERVER_WRITE_LOCKED"));
				System.out.println("Operation is not successful. Only get is available, please try again later.");
			}else{
				kvms = new KVMessageStorage(tokens[1], null, StatusTypeLookup(tokens[0]));
			}
		}else{
			if(value.getBytes().length <= 120000){
				sb.append(" ");
				sb.append(value);
			}else{
				logger.info("value exceeds max length 120kBytes");
				System.out.println("value exceeds max length 120 kbytes");
				kvms = new KVMessageStorage(null,null, StatusTypeLookup("PUT_ERROR"));
				return kvms;
			}

			msg = new TextMessage(sb.toString());
			sendMessage(new TextMessage(sb.toString()));
			TextMessage res = receiveMessage();

			String[] arr = res.getMsg().split("\\s+",2);
			if(StatusTypeLookup(arr[0]) == KVMessage.StatusType.SERVER_NOT_RESPONSIBLE){
				kvms = new KVMessageStorage(null,arr[1],StatusTypeLookup("SERVER_NOT_RESPONSIBLE"));
			}else if (StatusTypeLookup(arr[0]) == KVMessage.StatusType.SERVER_STOPPED){
				kvms = new KVMessageStorage(null,null, StatusTypeLookup("SERVER_STOPPED"));
				System.out.println("Operation is not successful, please try again later.");
			}else if (StatusTypeLookup(arr[0]) == KVMessage.StatusType.SERVER_WRITE_LOCK){
				kvms = new KVMessageStorage(null,null, StatusTypeLookup("SERVER_WRITE_LOCKED"));
				System.out.println("Operation is not successful. Only get is available, please try again later.");
			}else{
				String[] tokens = res.getMsg().split("\\s+",3);
				System.out.println("KEY: " + key.trim());
				System.out.println("VALUE: " + value.trim());
				System.out.println("STATUS: " + tokens[0].trim());
				kvms = new KVMessageStorage(tokens[1], tokens[2], StatusTypeLookup(tokens[0]));
			}
		}

		return kvms;
	}

	@Override
	public KVMessage get(String key) throws Exception {
		// TODO Auto-generated method stub

		KVMessageStorage kvms = null;
		StringBuilder sb = new StringBuilder();
		sb.append("get ");
		if(key.getBytes().length <= 20){
			sb.append(key);
		}else{
			logger.info("key exceeds max length 20 bytes");
			System.out.println("key exceeds max length 20 bytes");
			kvms = new KVMessageStorage(null, null, StatusTypeLookup("GET_ERROR"));
			return kvms;
		}

		sendMessage(new TextMessage(sb.toString()));
		TextMessage res = receiveMessage();

		String[] arr = res.getMsg().split("\\s+",2);

		if(StatusTypeLookup(arr[0]) == KVMessage.StatusType.SERVER_NOT_RESPONSIBLE){
			kvms = new KVMessageStorage(null,arr[1],StatusTypeLookup("SERVER_NOT_RESPONSIBLE"));
		}else if (StatusTypeLookup(arr[0]) == KVMessage.StatusType.SERVER_STOPPED){
			kvms = new KVMessageStorage(null,null, StatusTypeLookup("SERVER_STOPPED"));
			System.out.println("STATUS: " +  arr[0]);
		}else if (StatusTypeLookup(arr[0]) == KVMessage.StatusType.SERVER_WRITE_LOCK){
			kvms = new KVMessageStorage(null,null, StatusTypeLookup("SERVER_WRITE_LOCKED"));
			System.out.println("STATUS: " +  arr[0]);
		}else{
			String[] tokens = res.getMsg().split("\\s+",3);
			kvms = new KVMessageStorage(tokens[1],tokens[2], StatusTypeLookup(tokens[0]));
			System.out.println("KEY: " + tokens[1]);
			System.out.println("VALUE: " + tokens[2]);
			System.out.println("STATUS: " +  tokens[0]);
		}

		return kvms;

	}

	/**
	 * method for subscribe command
	 * command format: subscribe <key> <port>
	 *     expected output format: <status> <key if successful or reason why it's not successful>
	 * @param key key the client is subscribing
	 * @param port localhost will be listening on this port if any update
	 *             this port number is created once client is init'ed
	 * @return
	 * @throws Exception
	 */

	public KVMessage subscribe(String key, int port) throws Exception {

		KVMessageStorage kvms = null;
		StringBuilder sb = new StringBuilder();
		sb.append("subscribe ");
		if(key.getBytes().length <= 20){
			sb.append(key);
			sb.append(" ");
			sb.append(port);
		}else{
			logger.info("key exceeds max length 20 bytes");
			System.out.println("key exceeds max length 20 bytes");
			kvms = new KVMessageStorage(null, null, StatusTypeLookup("SUBSCRIBE_ERROR"));
			return kvms;
		}

		sendMessage(new TextMessage(sb.toString()));
		TextMessage res = receiveMessage();

		String[] arr = res.getMsg().split("\\s+",2);

		if(StatusTypeLookup(arr[0]) == KVMessage.StatusType.SERVER_NOT_RESPONSIBLE){
			kvms = new KVMessageStorage(null,arr[1],StatusTypeLookup("SERVER_NOT_RESPONSIBLE"));
		}else if (StatusTypeLookup(arr[0]) == KVMessage.StatusType.SERVER_STOPPED){
			kvms = new KVMessageStorage(null,null, StatusTypeLookup("SERVER_STOPPED"));
			System.out.println("STATUS: " +  arr[0]);
		}else if (StatusTypeLookup(arr[0]) == KVMessage.StatusType.SERVER_WRITE_LOCK){
			kvms = new KVMessageStorage(null,null, StatusTypeLookup("SERVER_WRITE_LOCKED"));
			System.out.println("STATUS: " +  arr[0]);
		}else if(StatusTypeLookup(arr[0]) == KVMessage.StatusType.SUBSCRIBE_ERROR){
			kvms = new KVMessageStorage(null,null, StatusTypeLookup("SUBSCRIBE_ERROR"));
			System.out.println("STATUS: " +  arr[0]);
			System.out.println(arr[1]); //reason why subscribe is not successful
		}else /* if(StatusTypeLookup(arr[0]) == KVMessage.StatusType.SUBSCRIBE_SUCCESS)*/ {
			kvms = new KVMessageStorage(null,null, StatusTypeLookup("SUBSCRIBE_SUCCESS"));
			System.out.println("STATUS: " +  arr[0]);
			String[] kvSet = arr[1].split(" ", 2);
			System.out.println("Subscribed to key: " + kvSet[0] + " with current value: " + kvSet[1]);
		}

		return kvms;

	}

	/**
	 * method to unsubscribe a key
	 * command format: unsubscribe <key> <port>
	 *     expected output format: <status> <key if successful or reason why it's not successful>
	 * @param key
	 * @param port port number will be the same number client sent in subscribe
	 * @return
	 * @throws Exception
	 */
	public KVMessage unsubscribe(String key, int port) throws Exception {

		KVMessageStorage kvms = null;
		StringBuilder sb = new StringBuilder();
		sb.append("unsubscribe ");
		if(key.getBytes().length <= 20){
			sb.append(key);
			sb.append(" ");
			sb.append(port);
		}else{
			logger.info("key exceeds max length 20 bytes");
			System.out.println("key exceeds max length 20 bytes");
			kvms = new KVMessageStorage(null, null, StatusTypeLookup("SUBSCRIBE_ERROR"));
			return kvms;
		}

		sendMessage(new TextMessage(sb.toString()));
		TextMessage res = receiveMessage();

		String[] arr = res.getMsg().split("\\s+",2);

		if(StatusTypeLookup(arr[0]) == KVMessage.StatusType.SERVER_NOT_RESPONSIBLE){
			kvms = new KVMessageStorage(null,arr[1],StatusTypeLookup("SERVER_NOT_RESPONSIBLE"));
		}else if (StatusTypeLookup(arr[0]) == KVMessage.StatusType.SERVER_STOPPED){
			kvms = new KVMessageStorage(null,null, StatusTypeLookup("SERVER_STOPPED"));
			System.out.println("STATUS: " +  arr[0]);
		}else if (StatusTypeLookup(arr[0]) == KVMessage.StatusType.SERVER_WRITE_LOCK){
			kvms = new KVMessageStorage(null,null, StatusTypeLookup("SERVER_WRITE_LOCKED"));
			System.out.println("STATUS: " +  arr[0]);
		}else if(StatusTypeLookup(arr[0]) == KVMessage.StatusType.SUBSCRIBE_ERROR){
			kvms = new KVMessageStorage(null,null, StatusTypeLookup("UNSUBSCRIBE_ERROR"));
			System.out.println("STATUS: " +  arr[0]);
			System.out.println(arr[1]); //reason why unsubscribe is not successful
		}else /* if(StatusTypeLookup(arr[0]) == KVMessage.StatusType.SUBSCRIBE_SUCCESS)*/ {
			kvms = new KVMessageStorage(null,null, StatusTypeLookup("UNSUBSCRIBE_SUCCESS"));
			System.out.println("STATUS: " +  arr[0]);
			System.out.println("Unsubscribed from key: " + arr[1]);
		}

		return kvms;

	}



	/**
	 * Method sends a TextMessage using this socket.
	 * @param msg the message that is to be sent.
	 * @throws IOException some I/O error regarding the output stream
	 */
	public void sendMessage(TextMessage msg) throws IOException {
		byte[] msgBytes = msg.getMsgBytes();
		output.write(msgBytes, 0, msgBytes.length);
		output.flush();
		logger.info("Send message:\t '" + msg.getMsg() + "'");
	}

	public TextMessage receiveMessage() throws IOException {


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
		logger.info("Receive message:\t '" + msg.getMsg() + "'");

		return msg;
	}

	public KVMessage.StatusType StatusTypeLookup(String status){
		KVMessage.StatusType st = null;

		switch (status){
			case "GET":
				st = KVMessage.StatusType.GET;
				break;
			case "GET_ERROR":
				st = KVMessage.StatusType.GET_ERROR;
				break;
			case "GET_SUCCESS":
				st = KVMessage.StatusType.GET_SUCCESS;
				break;
			case "PUT":
				st = KVMessage.StatusType.PUT;
				break;
			case "PUT_SUCCESS":
				st = KVMessage.StatusType.PUT_SUCCESS;
				break;
			case "PUT_UPDATE":
				st = KVMessage.StatusType.PUT_UPDATE;
				break;
			case "PUT_ERROR":
				st = KVMessage.StatusType.PUT_ERROR;
				break;
			case "DELETE_SUCCESS":
				st = KVMessage.StatusType.DELETE_SUCCESS;
				break;
			case "DELETE_ERROR":
				st = KVMessage.StatusType.DELETE_ERROR;
				break;
			case "SERVER_STOPPED":
				st = KVMessage.StatusType.SERVER_STOPPED;
				break;
			case "SERVER_WRITE_LOCK":
				st = KVMessage.StatusType.SERVER_WRITE_LOCK;
				break;
			case "SERVER_NOT_RESPONSIBLE":
				st = KVMessage.StatusType.SERVER_NOT_RESPONSIBLE;
				break;
			case "SUBSCRIBE_SUCCESS":
				st = KVMessage.StatusType.SUBSCRIBE_SUCCESS;
				break;
			case "SUBSCRIBE_ERROR":
				st = KVMessage.StatusType.SUBSCRIBE_ERROR;
				break;
			case "UNSUBSCRIBE_SUCCESS":
				st = KVMessage.StatusType.UNSUBSCRIBE_SUCCESS;
				break;
			case "UNSUBSCRIBE_ERROR":
				st = KVMessage.StatusType.UNSUBSCRIBE_ERROR;
				break;
		}
		return st;
	}
}