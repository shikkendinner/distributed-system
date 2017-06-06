package app_kvServer;

import common.messages.KVMessage;

public class KVMessageStorage implements KVMessage{

    private String key;
    private String value;
    private StatusType status;


    public KVMessageStorage(String key, String value, StatusType status){

        this.key = key;
        this.value = value;
        this.status = status;
    }

    @Override
    public String getKey() {
        return key;
    }

    @Override
    public String getValue() {
        return value;
    }

    @Override
    public StatusType getStatus() {
        return status;
    }

}
