package common.messages;

public interface KVAdminMessage {
	
    public enum StatusType {
    	INITIALIZATION_SUCCESS,         /* Initialization was successful*/
    	SERVER_STARTED,         /* Starting server was successful*/
    	SERVER_STOPPED,         /* Stopping server was successful*/
    	SERVER_SHUTDOWN_COMPLETE,         /* Shutting down server was successful*/
    	LOCK_WRITE_SUCCESSFUL,         /* Locking the write operation was successful*/
    	UNLOCK_WRITE_SUCCESSFUL,		/* Unlocking the write operation was successful*/
    	METADATA_UPDATE_SUCCESSFUL,         /* Updating the metadata on the server was successful*/
    	METADATA_UPDATE_FAILED,				/* Updating the metadat on the server broke down*/
    	DATA_TRANSFER_SUCCESSFUL,         /* Data arrived at the target server successfully*/
    	DATA_TRANSFER_FAILED,         /* Data did not arrive at the server, try again*/
    	DELETING_KVPAIRS_FAILED,	/* The key-value pairs could not be all deleted*/
    	DELETING_KVPAIRS_SUCCESSFUL,	/* The key-value pairs were all deleted*/
    	REPLICATION_SUCCESSFUL, /* Data was replicated to both replicas successfully*/
    	REPLICATION_FAILED, /* Data was not replicated to both replicas properly*/
    	MOVING_REPLICA_DATA_TO_MAIN_FILE_SUCCESSFUL, /* In an event of a failure moving the replica file data to the main storage file was successful*/
    	MOVING_REPLICA_DATA_TO_MAIN_FILE_FAILED, /* In an event of a failure moving the replica file data to the main storage file failed*/
    	HEARTBEAT_RECEIVED, /* Received heartbeat from ecs, reply back to notify server is alive*/
	}

	/**
	 * @return a status string that is used to identify request types, 
	 * response types and error types associated to the message.
	 */
	public StatusType getStatus();
	
	/**
	 * @return a String with the appropriate data, if needed
	 */
	public String getData();
	
}


