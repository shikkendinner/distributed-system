package app_kvServer;

import common.messages.KVAdminMessage;

public class KVAdminMessageStorage implements KVAdminMessage{

    private StatusType status;
    private String data;


    public KVAdminMessageStorage(StatusType status, String data){

        this.status = status;
        this.data = data;
    }

    @Override
    public StatusType getStatus() {
        return status;
    }
    
    @Override
    public String getData() {
        return data;
    }

}
