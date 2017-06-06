package testing;

import app_kvEcs.ConsistentHashing;
import app_kvEcs.Ecs;
import app_kvServer.KVServer;
import org.junit.Assert;
import org.junit.Test;

import client.KVStore;
import client.Client;
import client.Metadata;
import client.Address;
import junit.framework.TestCase;
import common.messages.KVMessage;

import java.io.File;
import java.io.IOException;
import java.io.PrintWriter;
import java.net.ServerSocket;
import java.nio.charset.Charset;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.*;
import common.messages.KVMessage;
import common.messages.KVMessage.StatusType;


public class AdditionalTest extends TestCase {

	// TODO add your test cases, at least 3
	int CACHE_SIZE =5;
	String CACHE_STRATEGY = "FIFO";
	int PORT = 3000;
	int ptLimit = 100;

	/**
	 * The best test we got
	 */
	@Test
	public void testStub() {
		assertTrue(true);
	}

	/**
	 * client test
	 * test if the client is able to create metaData
	 */
	@Test
	public void testCreateMetadata(){
		Address address = null;
		Exception ex = null;

		try {
			String metadata = "b7b2df4f3ec9d6dfccfa64a0e0993b0e,c98f070ad535b44eb9ff4469c98a00f3," +
					"127.0.0.1,50014,127.0.0.1,50011,127.0.0.1,50010 " +
					"c98f070ad535b44eb9ff4469c98a00f3,3ebf39bfa08189651d170e593782fea4," +
					"127.0.0.1,50011,127.0.0.1,50010,127.0.0.1,50016 " +
					"51feb15dc634eb38a2be7d96b6cf6fa5,68400e786fac12b87965d436018373d0," +
					"127.0.0.1,50016,127.0.0.1,50017,127.0.0.1,50015 " +
					"856cb2d0c1483bcefbcebf14b586d682,97afa5de704f8bf6297962a5f800476d," +
					"127.0.0.1,50015,127.0.0.1,50012,127.0.0.1,50013 " +
					"97afa5de704f8bf6297962a5f800476d,a6dc40fc074b383f29d1c75aaac1b6c4," +
					"127.0.0.1,50012,127.0.0.1,50013,127.0.0.1,50014 " +
					"a6dc40fc074b383f29d1c75aaac1b6c4,b7b2df4f3ec9d6dfccfa64a0e0993b0e," +
					"127.0.0.1,50013,127.0.0.1,50014,127.0.0.1,50011 " +
					"68400e786fac12b87965d436018373d0,856cb2d0c1483bcefbcebf14b586d682," +
					"127.0.0.1,50017,127.0.0.1,50015,127.0.0.1,50012 " +
					"3ebf39bfa08189651d170e593782fea4,51feb15dc634eb38a2be7d96b6cf6fa5," +
					"127.0.0.1,50010,127.0.0.1,50016,127.0.0.1,50017";
			Metadata mData = new Metadata();
			mData.updateMetadata(metadata);
			address = mData.lookup("key");

		} catch (Exception e) {
			ex = e;
		}

		assertTrue(ex == null && address.getPort() == 50011);
	}

	/**
	 * client test
	 * Test if the client is able to add an entry to metaData
	 */
	@Test
	public void testAddMetaData(){
		Address address = null;
		Exception ex = null;

		try {
			String metadata = "b7b2df4f3ec9d6dfccfa64a0e0993b0e,c98f070ad535b44eb9ff4469c98a00f3," +
					"127.0.0.1,50014,127.0.0.1,50011,127.0.0.1,50010 " +
					"51feb15dc634eb38a2be7d96b6cf6fa5,68400e786fac12b87965d436018373d0," +
					"127.0.0.1,50016,127.0.0.1,50017,127.0.0.1,50015 " +
					"856cb2d0c1483bcefbcebf14b586d682,97afa5de704f8bf6297962a5f800476d," +
					"127.0.0.1,50015,127.0.0.1,50012,127.0.0.1,50013 " +
					"97afa5de704f8bf6297962a5f800476d,a6dc40fc074b383f29d1c75aaac1b6c4," +
					"127.0.0.1,50012,127.0.0.1,50013,127.0.0.1,50014 " +
					"a6dc40fc074b383f29d1c75aaac1b6c4,b7b2df4f3ec9d6dfccfa64a0e0993b0e," +
					"127.0.0.1,50013,127.0.0.1,50014,127.0.0.1,50011 " +
					"68400e786fac12b87965d436018373d0,856cb2d0c1483bcefbcebf14b586d682," +
					"127.0.0.1,50017,127.0.0.1,50015,127.0.0.1,50012 " +
					"3ebf39bfa08189651d170e593782fea4,51feb15dc634eb38a2be7d96b6cf6fa5," +
					"127.0.0.1,50010,127.0.0.1,50016,127.0.0.1,50017";
			Metadata mData = new Metadata();
			mData.updateMetadata(metadata);
			mData.add("127.0.0.1 50011");
			address = mData.lookup("key");

		} catch (Exception e) {
			ex = e;
		}

		assertTrue(ex == null && address.getPort() == 50011);
	}

	/**
	 * client test
	 * Test if the client is able to get the first replica
	 */
	@Test
	public void testGetFirstReplicas(){
		Address[] addresses = null;
		Exception ex = null;

		try {
			String metadata = "b7b2df4f3ec9d6dfccfa64a0e0993b0e,c98f070ad535b44eb9ff4469c98a00f3," +
					"127.0.0.1,50014,127.0.0.1,50011,127.0.0.1,50010 " +
					"c98f070ad535b44eb9ff4469c98a00f3,3ebf39bfa08189651d170e593782fea4," +
					"127.0.0.1,50011,127.0.0.1,50010,127.0.0.1,50016 " +
					"51feb15dc634eb38a2be7d96b6cf6fa5,68400e786fac12b87965d436018373d0," +
					"127.0.0.1,50016,127.0.0.1,50017,127.0.0.1,50015 " +
					"856cb2d0c1483bcefbcebf14b586d682,97afa5de704f8bf6297962a5f800476d," +
					"127.0.0.1,50015,127.0.0.1,50012,127.0.0.1,50013 " +
					"97afa5de704f8bf6297962a5f800476d,a6dc40fc074b383f29d1c75aaac1b6c4," +
					"127.0.0.1,50012,127.0.0.1,50013,127.0.0.1,50014 " +
					"a6dc40fc074b383f29d1c75aaac1b6c4,b7b2df4f3ec9d6dfccfa64a0e0993b0e," +
					"127.0.0.1,50013,127.0.0.1,50014,127.0.0.1,50011 " +
					"68400e786fac12b87965d436018373d0,856cb2d0c1483bcefbcebf14b586d682," +
					"127.0.0.1,50017,127.0.0.1,50015,127.0.0.1,50012 " +
					"3ebf39bfa08189651d170e593782fea4,51feb15dc634eb38a2be7d96b6cf6fa5," +
					"127.0.0.1,50010,127.0.0.1,50016,127.0.0.1,50017";
			Metadata mData = new Metadata();
			mData.updateMetadata(metadata);
			addresses = mData.getReplicas("127.0.0.1:50010");

		} catch (Exception e) {
			ex = e;
		}

		assertTrue(ex == null && addresses[0].getPort() == 50016);
	}


	/**
	 * client test
	 * Test if the client is able to get the second replica
	 */
	@Test
	public void testGetSecondReplicas(){
		Address[] addresses = null;
		Exception ex = null;

		try {
			String metadata = "b7b2df4f3ec9d6dfccfa64a0e0993b0e,c98f070ad535b44eb9ff4469c98a00f3," +
					"127.0.0.1,50014,127.0.0.1,50011,127.0.0.1,50010 " +
					"c98f070ad535b44eb9ff4469c98a00f3,3ebf39bfa08189651d170e593782fea4," +
					"127.0.0.1,50011,127.0.0.1,50010,127.0.0.1,50016 " +
					"51feb15dc634eb38a2be7d96b6cf6fa5,68400e786fac12b87965d436018373d0," +
					"127.0.0.1,50016,127.0.0.1,50017,127.0.0.1,50015 " +
					"856cb2d0c1483bcefbcebf14b586d682,97afa5de704f8bf6297962a5f800476d," +
					"127.0.0.1,50015,127.0.0.1,50012,127.0.0.1,50013 " +
					"97afa5de704f8bf6297962a5f800476d,a6dc40fc074b383f29d1c75aaac1b6c4," +
					"127.0.0.1,50012,127.0.0.1,50013,127.0.0.1,50014 " +
					"a6dc40fc074b383f29d1c75aaac1b6c4,b7b2df4f3ec9d6dfccfa64a0e0993b0e," +
					"127.0.0.1,50013,127.0.0.1,50014,127.0.0.1,50011 " +
					"68400e786fac12b87965d436018373d0,856cb2d0c1483bcefbcebf14b586d682," +
					"127.0.0.1,50017,127.0.0.1,50015,127.0.0.1,50012 " +
					"3ebf39bfa08189651d170e593782fea4,51feb15dc634eb38a2be7d96b6cf6fa5," +
					"127.0.0.1,50010,127.0.0.1,50016,127.0.0.1,50017";
			Metadata mData = new Metadata();
			mData.updateMetadata(metadata);
			addresses = mData.getReplicas("127.0.0.1:50010");

		} catch (Exception e) {
			ex = e;
		}

		assertTrue(ex == null && addresses[1].getPort() == 50017);
	}


	/**
	 * Test client put and get with smaller number of servers
	 */
	@Test
	public void testPutGet(){

		File file = new File("./data/");
		deleteFolder(file);
		File dir = new File("./data/");
		dir.mkdir();
		Ecs ecs = new Ecs();
		ecs.readFile();
		ecs.participatingServers = new ArrayList<>();
		ecs.kvList = new ArrayList<>();
		ecs.notParticipating = new ArrayList<>();
		KVMessage kvMessage = null;
		ecs.initServiceTest(3, 10 ,"fifo");
		ecs.start();
		Exception putEx = null;
		try{

			Client client = new Client(ecs.participatingServers.get(0).split(" ")[0], Integer.valueOf(ecs.participatingServers.get(0).split(" ")[1]));
			kvMessage = client.putMessage("one","52");
		}catch(Exception e){
			e.printStackTrace();
			putEx = e;
		}

		assertTrue(putEx == null && kvMessage.getStatus()==KVMessage.StatusType.PUT_SUCCESS);

		KVMessage getMessage = null;
		Exception getEX = null;
		try{
			Client client = new Client(ecs.participatingServers.get(0).split(" ")[0], Integer.valueOf(ecs.participatingServers.get(0).split(" ")[1]));
			getMessage = client.getMessage("one");
		}catch (Exception e){
			e.printStackTrace();
			getEX = e;
		}

		ecs.shutdown();
		assertTrue(getEX == null && getMessage.getStatus() == KVMessage.StatusType.GET_SUCCESS);
	}

	/**
	 * Test if a client can get a deleted key with smaller number of servers
	 */
	@Test
	public void testDeleteGet(){

		File file = new File("./data/");
		deleteFolder(file);
		File dir = new File("./data/");
		dir.mkdir();
		Ecs ecs = new Ecs();
		ecs.readFile();
		ecs.participatingServers = new ArrayList<>();
		ecs.kvList = new ArrayList<>();
		ecs.notParticipating = new ArrayList<>();
		KVMessage kvMessage = null;
		ecs.initServiceTest(3, 10 ,"fifo");
		ecs.start();
		Exception putEx = null;
		try{

			Client client = new Client(ecs.participatingServers.get(0).split(" ")[0], Integer.valueOf(ecs.participatingServers.get(0).split(" ")[1]));
			kvMessage = client.putMessage("one","52");
			kvMessage = client.putMessage("one","null");
		}catch(Exception e){
			e.printStackTrace();
			putEx = e;
		}

		assertTrue(putEx == null && kvMessage.getStatus()==KVMessage.StatusType.DELETE_SUCCESS);

		KVMessage getMessage = null;
		Exception getEX = null;
		try{
			Client client = new Client(ecs.participatingServers.get(0).split(" ")[0], Integer.valueOf(ecs.participatingServers.get(0).split(" ")[1]));
			getMessage = client.getMessage("one");
		}catch (Exception e){
			e.printStackTrace();
			getEX = e;
		}

		ecs.shutdown();
		assertTrue(getEX == null && getMessage.getStatus() == StatusType.GET_ERROR);
	}



	/**
	 * put <key><value>, <key> has a max length of 20 bytes
	 * <value> has a max length of 120kBytes
	 * check if a put-operation with a large key returns error
	 *
	 */
	@Test
	public void testPutLargeKey() {
		String key = "averylargekeythatisbiggerthantwentybytes";
		String value = "ok";

		KVMessage response = null;
		Exception ex = null;

		try {
			KVStore kvClient = new KVStore("localhost",50000);
			response = kvClient.put(key, value);

		} catch (Exception e) {
			ex = e;
		}

		assertTrue(ex == null && response.getStatus() == KVMessage.StatusType.PUT_ERROR);
	}

	/**
	 * get <key>, <key> has a max length of 20 bytes
	 * check if a get-operation with a large key returns error
	 *
	 */
	@Test
	public void testGetLargeKey() {
		String key = "averylargekeythatisbiggerthantwentybytes";
		String value = "ok";

		KVMessage response = null;
		Exception ex = null;

		try {
			KVStore kvClient = new KVStore("localhost",50000);
			response = kvClient.get(key);

		} catch (Exception e) {
			ex = e;
		}

		assertTrue(ex == null && response.getStatus() == KVMessage.StatusType.GET_ERROR);
	}

	public static void clearFile(int port){

		PrintWriter pw = null;
		try {
			String fileName = "./data/storage" + port + ".txt";
			pw = new PrintWriter(fileName);
			pw.close();

		} catch (IOException e) {
			e.printStackTrace();
		}

	}

	public static void deleteFolder(File folder) {
		File[] files = folder.listFiles();
		if(files!=null) { //some JVMs return null for empty dirs
			for(File f: files) {
				if(f.isDirectory()) {
					deleteFolder(f);
				} else {
					f.delete();
				}
			}
		}
		folder.delete();
	}

	/**
	 * testSSH tests for:
	 * 	- ssh functionality
	 * 	- connect functionality
	 */
	@Test
	public void testSSH(){
		String ip = "127.0.0.1";
		String port = "50010";
		int ecsPort = 40000;
		Ecs ecs = new Ecs();
		Process proc = null;
		String script = "src/app_kvEcs/script.sh";
		String[] cmd = {"sh", script, ip, port};
		Exception exception = null;

		Runtime run = Runtime.getRuntime();
		try {
			proc = run.exec(cmd);

			//wait till the server notifies that it is about to listen for connections, then proceed
			ServerSocket ss = new ServerSocket(ecsPort);
			ss.accept();

			ss.close();

		}
		catch (IOException e) {
			exception = e;
		}
		finally {
			if (proc != null) {
				proc.destroy();

				try {
					//test is looking for a response only
					ecs.connect(ip, Integer.parseInt(port), "ecs test");

				}
				catch (Exception e) {
					exception = e;
				}
			}
		}

		assertTrue(exception==null);

		try {
			ecs.connect(ip.trim(), Integer.valueOf(port), "ecs fail");
		}
		catch (Exception e) {
			exception = e;
		}

		assertTrue(exception==null);
	}

	/**
	 * Initialize an shutdown 3 servers
	 */

	@Test
	public void testShutdown(){

		Ecs ecs = new Ecs();
		ecs.readFile();
		ecs.participatingServers = new ArrayList<>();
		ecs.kvList = new ArrayList<>();
		ecs.notParticipating = new ArrayList<>();
		System.out.println("pre for loop in the test shutdown");
		for (int i = 0; i < 4; i++){
			System.out.println("for loop: "+ecs.mIpAndPorts.get(i) );

			ecs.participatingServers.add(ecs.mIpAndPorts.get(i));
			String ip = ecs.mIpAndPorts.get(i).split(" ")[0];
			String port = ecs.mIpAndPorts.get(i).split(" ")[1];
			ecs.ssh(ip, port, "ecs initkvserver "
					+ 10
					+ " "
					+ "fifo"
					+ " "
					+"3ebf39bfa08189651d170e593782fea4,51feb15dc634eb38a2be7d96b6cf6fa5,"
					+
					"127.0.0.1,50010,127.0.0.1,50014,127.0.0.1,50011 "
					+
					"51feb15dc634eb38a2be7d96b6cf6fa5,c98f070ad535b44eb9ff4469c98a00f3,"
					+
					"127.0.0.1,50014,127.0.0.1,50011,127.0.0.1,50010 "
					+
					"c98f070ad535b44eb9ff4469c98a00f3,3ebf39bfa08189651d170e593782fea4,"
					+
					"127.0.0.1,50011,127.0.0.1,50010,127.0.0.1,50014");
		}

		System.out.println("before shutdown");
		ecs.shutdown();
		System.out.println("Post shut down");
		assertTrue(ecs.cs == null);
		assertTrue(ecs.metadata == null);
		assertTrue(ecs.participatingServers.size() == 0);
		assertTrue(ecs.notParticipating.size() == 0);


	}

	/**
	 * testInitService tests whether the ecs can initialize the right number of servers correctly
	 */
	@Test
	public void testInitService(){
		Ecs ecs = new Ecs();
		ecs.readFile();
		ecs.initServiceTest(3,10,"fifo");
		Exception exception = null;
		ecs.kvList = new ArrayList<>();
		List<String> participatingServers = ecs.participatingServers;

		for (int i = 0; i < participatingServers.size(); i++){

			String partIpAddress = participatingServers.get(i).split(" ")[0];
			int partPort = Integer.valueOf(participatingServers.get(i).split(" ")[1]);

			try {
				ecs.connect(partIpAddress, partPort, "ecs random");
			}
			catch (Exception e) {
				exception = e;
				e.printStackTrace();
			}

		}

		ecs.shutdown();

		assertTrue(exception==null);


	}

	@Test
	public void testComputeMetadata(){

		Ecs ecs = new Ecs();
		ecs.readFile();
		ecs.participatingServers = new ArrayList<>();

		for (int i = 0; i < 3; i++) ecs.participatingServers.add(ecs.mIpAndPorts.get(i));

		ecs.cs = new ConsistentHashing(1, ecs.participatingServers);
		String metadata = ecs.computeMetadata(ecs.participatingServers);
		String[] mdParts = metadata.split(" ");

		//each servers metadata is split by a space and so must have three items
		assertTrue(mdParts.length == 3);

		//now test the contents
		ConsistentHashing.HashedServer hashedServer= ecs.cs.get("one");
		String start = hashedServer.mHashedKeys[0];
		String end = hashedServer.mHashedKeys[1];

		assertTrue(metadata.contains(start) && metadata.contains(end));
	}

	@Test
	public void testAddNode(){

		System.out.println("Starting add Node...");
		Ecs ecs = new Ecs();
		ecs.readFile();
		ecs.participatingServers = new ArrayList<>();
		ecs.kvList = new ArrayList<>();
		ecs.notParticipating = new ArrayList<>();

		System.out.println("calling initServTest");
		ecs.initServiceTest(3,10,"fifo");

		String metadata = ecs.computeMetadata(ecs.participatingServers);
		String[] mdParts = metadata.split(" ");

		//each servers metadata is split by a space and so must have three items
		assertTrue(mdParts.length == 3);

		ecs.addNode(10, "fifo");

		String updatedMd = ecs.computeMetadata(ecs.participatingServers);
		String[] updatedMdParts = updatedMd.split(" ");

		//each servers metadata is split by a space and so must have three items
		assertTrue(updatedMdParts.length == 4);

		ecs.shutdown();
	}

	@Test
	public void testDeleteNode(){
		Ecs ecs = new Ecs();
		ecs.readFile();
		ecs.participatingServers = new ArrayList<>();
		ecs.kvList = new ArrayList<>();
		ecs.notParticipating = new ArrayList<>();

		ecs.initService(4, 10, "fifo");


		String metadata = ecs.computeMetadata(ecs.participatingServers);
		String[] mdParts = metadata.split(" ");

		//each servers metadata is split by a space and so must have three items
		assertTrue(mdParts.length == 4);

		//get the server number
		String portToDel = ecs.participatingServers.get(0).split(" ")[1].trim();
		System.out.println(portToDel);
		int serverNum = 0;
		for (int i = 0; i < ecs.mIpAndPorts.size(); i++){
			if (ecs.mIpAndPorts.get(i).contains(portToDel)) serverNum = i+1;
		}

		System.out.println(serverNum);
		ecs.deleteNode(serverNum);

		String updatedMd = ecs.computeMetadata(ecs.participatingServers);
		String[] updatedMdParts = updatedMd.split(" ");

		assertTrue(updatedMdParts.length == 3);

		ecs.shutdown();

	}

	/**
	 * Begin M3 Tests
	 *
	 */

	@Test
	public void testFailureDetection(){
		List<Thread> hbThreads = new ArrayList<>();
		final Ecs ecs = new Ecs();
		ecs.readFile();
		ecs.participatingServers = new ArrayList<>();
		ecs.kvList = new ArrayList<>();
		ecs.notParticipating = new ArrayList<>();
		final Exception[] exception = {null};

		ecs.initServiceTest(3,10,"fifo");

		for ( int i =0; i < ecs.participatingServers.size(); i++){

			final int finalI = i;
			Thread thread = new Thread(new Runnable() {
				@Override
				public void run() {
					boolean failureDetected = false;
					while (!failureDetected){
						try {
							ecs.connectHeartBeat(ecs.participatingServers.get(finalI).split(" ")[0], Integer.valueOf(ecs.participatingServers.get(finalI).split(" ")[1]), "ecs heartbeat");
						}
						catch (Exception e) {
							failureDetected = true;
							exception[0] = e;
						}

						if (Thread.currentThread().isInterrupted()){
							break;
						}
					}
				}
			});

			hbThreads.add(thread);
			thread.start();

		}

//		//causes a server to system.exit on that server and the listening threads above would catch this exception.
//		ecs.testerShutdown(ecs.participatingServers.get(0).split(" ")[0],ecs.participatingServers.get(0).split(" ")[1]);
//
//		for (int i = 0; i < hbThreads.size(); i ++ ){
//
//			Thread th = hbThreads.get(i);
//			if (th!=null) th.interrupt();
//
//		}

		ecs.shutdown();
		//exception is only thrown when the error is detected
		assertTrue(exception!=null);

	}

	@Test
	public void testRecovery(){

		List<Thread> hbThreads = new ArrayList<>();
		final Ecs ecs = new Ecs();
		ecs.readFile();
		ecs.participatingServers = new ArrayList<>();
		ecs.kvList = new ArrayList<>();
		ecs.notParticipating = new ArrayList<>();
		final Exception[] exception = {null};

		ecs.initServiceTest(3,10,"fifo");
		ecs.cs = new ConsistentHashing(1, ecs.participatingServers);
		for ( int i =0; i < ecs.participatingServers.size(); i++){

			final int finalI = i;
			Thread thread = new Thread(new Runnable() {
				@Override
				public void run() {
					boolean failureDetected = false;
					while (!failureDetected){
						try {
							ecs.connectHeartBeat(ecs.participatingServers.get(finalI).split(" ")[0], Integer.valueOf(ecs.participatingServers.get(finalI).split(" ")[1]), "ecs heartbeat");
						}
						catch (Exception e) {
							failureDetected = true;
							exception[0] = e;
							String crashed = ecs.participatingServers.get(finalI);
							ecs.cs.remove(ecs.participatingServers.get(finalI));
							ecs.participatingServers.remove(ecs.participatingServers.get(finalI).split(" ")[0] + " "+ecs.participatingServers.get(finalI).split(" ")[1]);
							ecs.mIpAndPorts.remove(ecs.participatingServers.get(finalI).split(" ")[0] + " "+ecs.participatingServers.get(finalI).split(" ")[1])	;
							Random random = new Random();
							int radnInt = random.nextInt(ecs.mIpAndPorts.size());
							ecs.cs.add(ecs.mIpAndPorts.get(radnInt));
							ecs.participatingServers.add(ecs.mIpAndPorts.get(radnInt));
							String metaData = ecs.computeMetadata(ecs.participatingServers);
							assertTrue(!metaData.contains(crashed));
						}

						if (Thread.currentThread().isInterrupted()){
							break;
						}
					}
				}
			});
			hbThreads.add(thread);
			

		}

		//exception is only thrown when the error is detected
		assertTrue(exception!=null);
		ecs.shutdown();

	}

//	@Test
//	public void testPerformance() throws IOException {
//		File file = new File("./performanceTest/result.txt");
//		PrintWriter pw = new PrintWriter(file);
//
//		pw.println("3 servers, 1 client, cache size 128, fifo ------------- "
//				+ startServerClient(3,1,128,"fifo"));
//
//		pw.println("3 servers, 5 client, cache size 128, fifo ------------- "
//				+ startServerClient(3,5,128,"fifo"));
//
//		pw.println("5 servers, 1 client, cache size 128, fifo ------------- "
//				+ startServerClient(8,1,128,"fifo"));
//
//		pw.println("5 servers, 5 client, cache size 128, fifo ------------- "
//				+ startServerClient(8,5,128,"fifo"));
//
//
//		pw.println("3 servers, 1 client, cache size 128, lru ------------- "
//				+ startServerClient(3,1,128,"lru"));
//
//		pw.println("3 servers, 5 client, cache size 128, lru ------------- "
//				+ startServerClient(3,5,128,"lru"));
//
//		pw.println("5 servers, 1 client, cache size 128, lru ------------- "
//				+ startServerClient(8,1,128,"lru"));
//
//		pw.println("5 servers, 5 client, cache size 128, lru ------------- "
//				+ startServerClient(8,5,128,"lru"));
//
//
//		pw.println("3 servers, 1 client, cache size 128, lfu ------------- "
//				+ startServerClient(3,1,128,"lfu"));
//
//		pw.println("3 servers, 5 client, cache size 128, lfu ------------- "
//				+ startServerClient(3,5,128,"lfu"));
//
//		pw.println("5 servers, 1 client, cache size 128, lfu ------------- "
//				+ startServerClient(8,1,128,"lfu"));
//
//		pw.println("5 servers, 5 client, cache size 128, lfu ------------- "
//				+ startServerClient(8,5,128,"lfu"));
//
//		pw.close();
//	}

	public long startServerClient(int s, int c, int cacheSize, String strategy){
		long elapsedTime = 0;
		long startTime = System.currentTimeMillis();
		Ecs ecs = new Ecs();
		ecs.readFile();
		ecs.participatingServers = new ArrayList<>();
		ecs.kvList = new ArrayList<>();
		ecs.notParticipating = new ArrayList<>();
		KVMessage kvMessage = null;

		//create s server with given cacheSize and strategy
		ecs.initServiceTest(s, cacheSize ,strategy);
		ecs.start();
		Exception putEx = null;

		//start c client clients
		try{
			int i;
			for(i=0; i<c; i++){
				Client client = new Client(ecs.participatingServers.get(0).split(" ")[0], Integer.valueOf(ecs.participatingServers.get(0).split(" ")[1]));
				uploadFile("./src/testing/allen-p/_sent_mail/",client);
			}
		}catch(Exception e){
			e.printStackTrace();
		}
		long endTime = System.currentTimeMillis();
		return  endTime - startTime;
	}

	public void uploadFile(String path, Client client) throws Exception {
		File dir = new File(path);
		File[] directoryListing = dir.listFiles();
		if (directoryListing != null) {
			for (File child : directoryListing) {
				String content = readFile(child.toString(), Charset.defaultCharset());
				client.putMessage(child.getName(),content);
			}
		}
	}

	static String readFile(String path, Charset encoding)
			throws IOException
	{
		byte[] encoded = Files.readAllBytes(Paths.get(path));
		return new String(encoded, encoding);
	}
}
