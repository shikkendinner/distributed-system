package client;

/**
 * Created by JHUA on 2017-03-04.
 */
public class Address {
    private String ipAddr;
    private int port;

    public Address(String iAddr, int p){
        ipAddr = iAddr;
        port = p;
    }

    public String getIpAddr(){return ipAddr;}

    public int getPort(){return port;}
}
