package client;

/**
 * Created by JHUA on 2017-03-04.
 */
public interface ClientSocketListener {

    public enum SocketStatus{CONNECTED, DISCONNECTED, CONNECTION_LOST};

    public void handleNewMessage(TextMessage msg);

    public void handleStatus(SocketStatus status);
}

