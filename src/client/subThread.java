package client;

import app_kvServer.ClientConnectionKVServer;
import app_kvServer.TextMessageKVServer;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.io.OutputStream;
import java.io.PrintStream;
import java.net.ServerSocket;
import java.net.Socket;
import org.apache.log4j.Logger;

/**
 * Created by JHUA on 2017-04-12.
 */

public class subThread extends Thread {

    private String subAddress;
    private int subPort;
    private ServerSocket serverSocket;
    private Logger logger;
    private static final int BUFFER_SIZE = 1024;
    private static final int DROP_SIZE = 128 * BUFFER_SIZE;

    /**
     * constructor
     */
    public subThread(int port){
        this.subPort = port;
        this.serverSocket = null;
        this.logger = Logger.getRootLogger();
    }

    public void run(){
        try {
            //not bound yet
            if(serverSocket == null){
            	serverSocket = new ServerSocket(this.subPort);
            	
            	while(true){
                    Socket subscribeSocket = serverSocket.accept();
                    new Thread(new SubscriptionReceiver(subscribeSocket)).start();
                }
            }
        } catch (Exception e) {
            logger.error(e.getMessage());
        }
    }

    public void shutdown() throws IOException {
    	try{
    		if(serverSocket != null){
                serverSocket.close();
            }
    	} catch(Exception e){
    		//actually an exception we want to catch, and thus ignore
    	}
    }
    
    private class SubscriptionReceiver implements Runnable{
		private Socket socket = null;
		private InputStream input = null;
	    private OutputStream output = null;
    	
    	SubscriptionReceiver(Socket socket){
			this.socket = socket;
			try {
				this.input = this.socket.getInputStream();
				this.output = this.socket.getOutputStream();
			} catch (IOException e) {
	            logger.error(e.getMessage());
			}
		}
    	
    	@Override
		public void run() {
			try {
				PrintStream out = new PrintStream(output, true);
				BufferedReader in = new BufferedReader(new InputStreamReader(input));
				
				String msg = in.readLine().trim();
				String[] msgSplit = msg.split(" ");
				
				if(msgSplit.length == 2 && msgSplit[0].equals("change")){
					//then the key has been deleted
					System.out.println("The key: " + msgSplit[1] + " has been deleted!");
				} else if(msgSplit[0].equals("change")){
					//then the key has been modified
					msgSplit = msg.split(" ", 3);
					System.out.println("The key: " + msgSplit[1] + " has a new value: " + msgSplit[2]);
				}
				
				out.println("Got your message, now bye!");
			} catch (IOException e) {
				logger.error(e.getMessage());
			}
		}
    }
}