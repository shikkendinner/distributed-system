package client;

import app_kvServer.ClientConnectionKVServer;
import common.messages.KVMessage;
import org.apache.log4j.Logger;

import java.io.IOException;
import java.net.ServerSocket;
import java.net.Socket;
import java.net.UnknownHostException;
import java.util.Random;

/**
 * Created by JHUA on 2017-03-04.
 */
public class Client extends Thread{

    private boolean running;
    private KVStore kvStore;
    private static Logger logger = Logger.getRootLogger();
    private Metadata mData = new Metadata();
    private int subPort;
    private subThread myThread;

    /**
     *  Client constructor
     */
    public Client(String address, int port) throws UnknownHostException, IOException{
        kvStore = new KVStore(address, port);
        Random rand = new Random();
        subPort = rand.nextInt(5000) + 55000;
        myThread = null;
        try{
            kvStore.connect();
            setRunning(kvStore.getConnected());
        }catch(Exception e){
            e.printStackTrace();
        }

        logger.info("Connection established");
    }

    /**
     * Initializes and starts the client connection.
     * Loops until the connection is closed or aborted by the client.
     */
    public void run(){
        //logger.info("Client thread starts running");
        try{
            //currently it does nothing

            //if SERVER NOT RESPONSIBLE
                //update ListMetadata
                //create new kvStore

        }catch (Exception e){
            e.printStackTrace();
        }
    }

    public boolean isRunning(){
        return running;
    }

    public  void setRunning(boolean run){
        running = run;
    }


    public KVMessage putMessage(String key, String value) throws Exception {
        KVMessage kvm = kvStore.put(key,value);

        while(kvm.getStatus() == KVMessage.StatusType.SERVER_NOT_RESPONSIBLE){

            //update metadata
            mData.updateMetadata(kvm.getValue());

            //lookup
            Address address = mData.lookup(key);

            //close the wrong server
            kvStore.disconnect();

            //retry
            kvStore = new KVStore(address.getIpAddr(),address.getPort());
            kvStore.connect();
            kvm = kvStore.put(key,value);
        }
        
        return kvm;
    }

    public KVMessage getMessage(String key) throws Exception {
        KVMessage kvm = kvStore.get(key);

        while(kvm.getStatus() == KVMessage.StatusType.SERVER_NOT_RESPONSIBLE){
            //update metadata
            mData.updateMetadata(kvm.getValue());

            //lookup
            Address address = mData.lookup(key);
            Address[] replicas = mData.getReplicas(address.getIpAddr() + ":" + address.getPort());
            
            //pick either the primary or one of the replicas
            Random rand = new Random();
    		int index = rand.nextInt(3);
    		
    		if(index != 0){
    			address = replicas[index-1];
    		}
    		
            kvStore.disconnect();

            //retry
            kvStore = new KVStore(address.getIpAddr(),address.getPort());
            kvStore.connect();
            kvm = kvStore.get(key);
        }
        
        return kvm;
    }

    /**
     * method to subscribe a key
     * @param key
     * client is listening on a separate thread and prompt output if any update on the server
     * each client has one dedicated port for this task
     * port number range: 55000 - 59999
     */
    public KVMessage subMessage(String key) throws Exception {


        KVMessage kvm = kvStore.subscribe(key, subPort);

        if(this.myThread == null){
            this.myThread = new subThread(subPort);
            this.myThread.start();
        }

        while(kvm.getStatus() == KVMessage.StatusType.SERVER_NOT_RESPONSIBLE){
            //update metadata
            mData.updateMetadata(kvm.getValue());

            //lookup
            Address address = mData.lookup(key);
//            Address[] replicas = mData.getReplicas(address.getIpAddr() + ":" + address.getPort());
//
//            //pick either the primary or one of the replicas
//            Random rand = new Random();
//            int index = rand.nextInt(3);
//
//            if(index != 0){
//                address = replicas[index-1];
//            }

            kvStore.disconnect();

            //retry
            kvStore = new KVStore(address.getIpAddr(),address.getPort());
            kvStore.connect();
            kvm = kvStore.subscribe(key,subPort);
        }

        return kvm;
    }

    /**
     * method to unsubscribe a key
     * @param key
     * client is listening on a separate thread and prompt output if any update on the server
     * each client has one dedicated port for this task
     * port number range: 55000 - 59999
     */
    public KVMessage unsubMessage(String key) throws Exception {

        KVMessage kvm = kvStore.unsubscribe(key, subPort);

        while(kvm.getStatus() == KVMessage.StatusType.SERVER_NOT_RESPONSIBLE){
            //update metadata
            mData.updateMetadata(kvm.getValue());

            //lookup
            Address address = mData.lookup(key);
//            Address[] replicas = mData.getReplicas(address.getIpAddr() + ":" + address.getPort());
//
//            //pick either the primary or one of the replicas
//            Random rand = new Random();
//            int index = rand.nextInt(3);
//
//            if(index != 0){
//                address = replicas[index-1];
//            }

            kvStore.disconnect();

            //retry
            kvStore = new KVStore(address.getIpAddr(),address.getPort());
            kvStore.connect();
            kvm = kvStore.subscribe(key,subPort);
        }

        return kvm;
    }

    public void disconnect() throws IOException {
        kvStore.disconnect();
        setRunning(false);
        if(myThread != null){
            myThread.shutdown();
        }
    }
}