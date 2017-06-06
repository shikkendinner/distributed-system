package testing;

import org.junit.Test;

import client.KVStore;
import junit.framework.TestCase;
import common.messages.KVMessage;
import common.messages.KVMessage.StatusType;
import client.Client;

import java.io.IOException;


public class InteractionTest extends TestCase {

	private KVStore kvClient;
	private Client client;
	
	public void setUp() {
		kvClient = new KVStore("localhost", 50010);

		try {
			//kvClient.connect();
            client = new Client("localhost",50014);
		} catch (Exception e) {
		}
	}

	public void tearDown() {
		kvClient.disconnect();
	}
	
	
	@Test
	public void testPut() {
		String key = "putfoo";
		String value = "putbar";
		KVMessage response = null;
		Exception ex = null;

		try {
			//response = kvClient.put(key, value);
            response = client.putMessage(key,value);
		} catch (Exception e) {
			ex = e;
		}
		System.out.print("+++++++++++++++++" + response.getStatus());
		assertTrue(ex == null && response.getStatus() == StatusType.PUT_SUCCESS);
	}
	
	@Test
	public void testPutDisconnected() throws IOException {
		//kvClient.disconnect();
        client.disconnect();
		String key = "foo";
		String value = "bar";
		Exception ex = null;

		try {
			//kvClient.put(key, value);
            client.putMessage(key,value);
		} catch (Exception e) {
			ex = e;
		}

		assertNotNull(ex);
	}

	@Test
	public void testUpdate() {
		String key = "updateTestValue";
		String initialValue = "initial";
		String updatedValue = "updated";
		
		KVMessage response = null;
		Exception ex = null;

		try {
			//kvClient.put(key, initialValue);
			//response = kvClient.put(key, updatedValue);
			client.putMessage(key,initialValue);
			response = client.putMessage(key,updatedValue);
		} catch (Exception e) {
			ex = e;
		}

		assertTrue(ex == null && response.getStatus() == StatusType.PUT_UPDATE
				&& response.getValue().equals(updatedValue));
	}
	
	@Test
	public void testDelete() {
		String key = "deleteTestValue";
		String value = "toDelete";
		
		KVMessage response = null;
		Exception ex = null;

		try {
			//kvClient.put(key, value);
			//response = kvClient.put(key, "null");
			client.putMessage(key,value);
			response = client.putMessage(key,"null");
		} catch (Exception e) {
			ex = e;
		}

		assertTrue(ex == null && response.getStatus() == StatusType.DELETE_SUCCESS);
	}
	
	@Test
	public void testGet() {
		String key = "foo";
		String value = "bar";
		KVMessage response = null;
		Exception ex = null;

			try {
				//kvClient.put(key, value);
				//response = kvClient.get(key);
                client.putMessage(key,value);
                response = client.getMessage(key);
			} catch (Exception e) {
				ex = e;
			}
		
		assertTrue(ex == null && response.getValue().equals("bar"));
	}

	@Test
	public void testGetUnsetValue() {
		String key = "anunsetvalue";
		KVMessage response = null;
		Exception ex = null;

		try {
			//response = kvClient.get(key);
            response = client.getMessage(key);
		} catch (Exception e) {
			ex = e;
		}

		assertTrue(ex == null && response.getStatus() == StatusType.GET_ERROR);
	}
	


}
