package testing;

import client.Client;
import client.KVStore;
import common.messages.KVMessage;
import junit.framework.TestCase;
import org.junit.Test;

import java.io.IOException;

/**
 * Created by JHUA on 2017-04-13.
 */

public class SubscriptionTest extends TestCase {

    private KVStore kvClient;
    private Client client;

    public void setUp() {
        kvClient = new KVStore("127.0.0.1", 50010);

        try {
            //kvClient.connect();
            client = new Client("127.0.0.1",50014);
        } catch (Exception e) {
        }
    }

    public void tearDown() {
        kvClient.disconnect();
    }


    @Test
    public void testSubscribe() {
        String key = "subscribeKey";
        String value = "subscribeValue";


        KVMessage response = null;
        Exception ex = null;

        try {
            client.putMessage(key,value);
            response = client.subMessage(key);
        } catch (Exception e) {
            ex = e;
        }

        assertTrue(ex == null && response.getStatus() == KVMessage.StatusType.SUBSCRIBE_SUCCESS);
    }

    @Test
    public void testSubscribeNonExistingKey() {
        String key = "subNonExistingKey";

        KVMessage response = null;
        Exception ex = null;

        try {
            response = client.subMessage(key);
        } catch (Exception e) {
            ex = e;
        }

        assertTrue(ex == null && response.getStatus() == KVMessage.StatusType.SUBSCRIBE_ERROR);
    }


    @Test
    public void testUnsubscribe() {
        String key = "unsubscribeKey";
        String value = "unsubscribeValue";

        KVMessage response = null;
        Exception ex = null;

        try {
            client.putMessage(key,value);
            client.subMessage(key);
            response = client.unsubMessage(key);
        } catch (Exception e) {
            ex = e;
        }

        assertTrue(ex == null && response.getStatus() == KVMessage.StatusType.UNSUBSCRIBE_SUCCESS);
    }

    @Test
    public void testUnsubscribeNonExistingKey() {
        String key = "NonExistingKey";

        KVMessage response = null;
        Exception ex = null;

        try {
            response = client.unsubMessage(key);
        } catch (Exception e) {
            ex = e;
        }

        assertTrue(ex == null && response.getStatus() == KVMessage.StatusType.UNSUBSCRIBE_ERROR);
    }

    @Test
    public void testUnsubscribeKeyNotSubscribed() {
        String key = "KeyNotSubscribed";
        String value = "ValueNotSubscribed";

        KVMessage response = null;
        Exception ex = null;

        try {
            client.putMessage(key,value);
            response = client.unsubMessage(key);
        } catch (Exception e) {
            ex = e;
        }

        assertTrue(ex == null && response.getStatus() == KVMessage.StatusType.UNSUBSCRIBE_ERROR);
    }

    @Test
    public void testUnsubscribeKeyTwice() {
        String key = "unsubscribeKeyTwice";
        String value = "unsubscribeValueTwice";

        KVMessage response = null;
        Exception ex = null;

        try {
            client.putMessage(key,value);
            client.subMessage(key);
            client.unsubMessage(key);
            response = client.unsubMessage(key);
        } catch (Exception e) {
            ex = e;
        }

        assertTrue(ex == null && response.getStatus() == KVMessage.StatusType.UNSUBSCRIBE_ERROR);
    }

}