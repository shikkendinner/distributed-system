package client;

import app_kvEcs.ConsistentHashing;


import java.util.*;

/**
 * Created by JHUA on 2017-03-04.
 */

public class Metadata {
    List<String> list = new ArrayList<>();
    ConsistentHashing consistentHashing = new ConsistentHashing(1, list);


    //NavigableMap<Integer, MapValue> mList = new TreeMap<>();

    public Metadata(){}

    public void add(String entry){
        consistentHashing.add(entry);
        //System.out.println(entry);
        //System.out.println(consistentHashing.get(entry).mHashedKeys[0]);
        //System.out.println(consistentHashing.get(entry).mHashedKeys[0]);
    }

    /**
     * lookup function for metadata
     * if entry does not exist, returns null
     * otherwise return object that contains hashEnd, ip address and port number
     * @param key
     * @return
     */
    public Address lookup(String key){

        ConsistentHashing.HashedServer result = consistentHashing.get(key);

        String[] arr = result.mIpAndPort.split("\\s+");
        Address address = new Address(arr[0],Integer.parseInt(arr[1]));

        return  address;
    }


    /**
     * This function updates entire mList based on md
     * md contains completed information of mList, in the format of string
     * @param md
     */
    public void updateMetadata(String md){

        consistentHashing.circle.clear();

        String[] entry = md.split("\\s+");

        int i;
        for(i=0;i<entry.length;i++){
            String[] element = entry[i].split(",");
            add(element[2].trim() + " " + element[3].trim());
        }

    }
    
    public Address[] getReplicas(String ipPort){
    	String hash = consistentHashing.hashFunction(ipPort);
    	
    	String[] replicas = consistentHashing.getReplicas(hash);
    	String[] firstIpPort = replicas[0].split(" ");
    	String[] secondIpPort = replicas[1].split(" ");
    	
    	Address[] replicaDuo = {new Address(firstIpPort[0], Integer.parseInt(firstIpPort[1])), 
        		new Address(secondIpPort[0], Integer.parseInt(secondIpPort[1]))};
    	
    	return replicaDuo;
    }
}
