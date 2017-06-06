Things that need to be modified:
	- ./src/app_kvEcs/script.sh modify path to go to the jar file for server : ms3-server.jar
	- run the following command to ensure SSH does not bother anyone: ssh-keygen -R 127.0.0.1
	- run ecs and enjoy!
	
Things to know:
	- the data folder at the base directory (the directory that comes up when you write the command cd)
	contains the data files for the servers instantiated by the ecs
	- the log folder in the same place also has the logs for the servers
	- the data folder in the ScalableStorageService-stub directory contains data that is persisted
	to the ecs once all servers are shutdown
	- likewise for the log, it contains the log for the ecs

	
