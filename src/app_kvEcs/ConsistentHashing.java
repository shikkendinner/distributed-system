package app_kvEcs;

import java.math.BigInteger;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.util.*;

import org.apache.log4j.Logger;

public class ConsistentHashing {
    private int numberOfReplicas;
    public TreeMap<String, HashedServer> circle = new TreeMap<String, HashedServer>();
    
    private static Logger logger = Logger.getRootLogger();

    public ConsistentHashing(int numberOfReplicas, List<String> nodes) {

        this.numberOfReplicas = numberOfReplicas;

        for (String node : nodes) {
            add(node);
        }

    }


    /**
     * Add a server to the circle
     * @param ipAndPort IP + Port No.
     */
    public void add(String ipAndPort) {
        for (int i = 0; i < numberOfReplicas; i++) {
            String hashString = ipAndPort.trim().replaceAll("\\s",":");
            String hash = hashFunction(hashString);
            circle.put(hash, new HashedServer(ipAndPort));
        }

        for (String keys: circle.keySet()){
            storeRanges(keys);
        }
    }

    public void storeRanges(String hash){
        SortedMap<String,HashedServer> headMap = circle.headMap(hash);

        //if the head map is empty your range is from the last existing key to the first

        String startHash = headMap.isEmpty() ? circle.lastKey() : headMap.lastKey();

        // endHash is it self

        HashedServer currentServer = circle.get(hash);
        currentServer.mHashedKeys[0] = startHash;
        currentServer.mHashedKeys[1] = hash;
    }

    /**
     * Removes a node from the circle
     * @param node IP + Port
     */
    public void remove(String node) {
        for (int i = 0; i < numberOfReplicas; i++) {
            String hashString = node.trim().replaceAll("\\s",":");
            circle.remove(hashFunction(hashString));
        }
        
        for (String keys: circle.keySet()){
            storeRanges(keys);
        }
    }

    /**
     * Gets a string for the IP ADDRESS + PORT of the server
     * associated with the KEY provided
     * @param key
     * @return
     */
    public HashedServer get(String key) {
        
    	if (circle.isEmpty()) {
            return null;
        }
        
        String hashString = key.trim().replaceAll("\\s",":");
        String hash = hashFunction(hashString);
        
        if (!circle.containsKey(hash)) {

            SortedMap<String, HashedServer> tailMap = circle.tailMap(hash);

            hash = tailMap.isEmpty() ? circle.firstKey() : tailMap.firstKey();
        }

        return circle.get(hash);

    }
    
    public String[] getReplicas(String hash){
    	//get the next hashValue stored after hash
    	String[] replicaAddr = new String[2];
    	
    	String firstHash = circle.higherKey(hash);
    	if (firstHash == null){
    		//no key higher, need to loop to the start in the circle
    		firstHash = circle.firstKey();
    	}
    	
    	//get the second hash value stored after hash
    	String secondHash = circle.higherKey(firstHash);
    	if (secondHash == null){
    		//no key higher, need to loop to the start in the circle
    		secondHash = circle.firstKey();
    	}
    	
    	replicaAddr[0] = circle.get(firstHash).mIpAndPort;
    	replicaAddr[1] = circle.get(secondHash).mIpAndPort;
    	
    	return replicaAddr;
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
            md.update(ipAndPort.getBytes());
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

    public class HashedServer {
        public String[] mHashedKeys;
        public String mIpAndPort;

        HashedServer(String ipAndPort){
            mHashedKeys = new String[2];
            mIpAndPort = ipAndPort;
        }

    }
}
