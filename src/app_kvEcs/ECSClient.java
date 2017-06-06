package app_kvEcs;

import java.io.IOException;

import logger.LogSetup;

import org.apache.log4j.Level;

public class ECSClient {
    public static void main (String[] args){
    	try {
			new LogSetup("./logs/ecs/ecs.log", Level.ERROR);
		} catch (IOException e) {
			System.out.println("Log could not be setup, no messages will be recorded");
		}
    	
        Ecs ecs= new Ecs();
        ecs.run();
    }
}
