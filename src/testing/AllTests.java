package testing;

import java.io.IOException;
import java.util.ArrayList;

import org.apache.log4j.Level;

import app_kvServer.KVServer;
import app_kvEcs.Ecs;
import junit.framework.Test;
import junit.framework.TestSuite;
import logger.LogSetup;
import org.apache.log4j.Logger;

public class AllTests {


	static {
		try {
		new LogSetup("logs/testing/test.log", Level.ERROR);

         Ecs ecs = new Ecs();
         ecs.readFile();
         ecs.kvList = new ArrayList<>();
		 ecs.initServiceTest(8,128,"fifo");
		 ecs.start();

		} catch (IOException e) {
			e.printStackTrace();
		}
	}
	
	
	public static Test suite() {
		TestSuite clientSuite = new TestSuite("Basic Storage ServerTest-Suite");
		clientSuite.addTestSuite(ConnectionTest.class);
		clientSuite.addTestSuite(InteractionTest.class); 
		clientSuite.addTestSuite(AdditionalTest.class);
		clientSuite.addTestSuite(SubscriptionTest.class);
		return clientSuite;
	}
	
}