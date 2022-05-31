import java.io.*;
import java.net.*;
import java.util.*;

public class MyClient {
    public static Socket clientSocket;
    public static DataOutputStream outputStream;
    public static BufferedReader inputStream;
    public static List<String> burstServers;
	public static String[] serverToSchedule;

	public static String[] lastSCHDServer;

    public static void main(String[] args){
		try {
			/*
				Initialises Neccessary Streams, Sockets and Performs Handshake with Client.
			*/
			clientSocket = new Socket("localhost", 50000);
			outputStream = new DataOutputStream(clientSocket.getOutputStream());
			inputStream = new BufferedReader(new InputStreamReader(clientSocket.getInputStream()));
            performHandshake();
			
			//Servers which are available Immediately(No Running Jobs) are names Burst Servers
		    burstServers = new ArrayList<>(); 
			int serverPointer = 0; //Pointer for a List of Arrays with Servers
			String str = "";
			boolean parseIntoBurstServers = true;//Boolean Flag to run conditional only once

			//Loop Keeps Running Until, None is Recieved.
			while (true) {
				
                send("REDY");
                str = recieve(); // JOBN Stored to Job.
				
				// Loop Handles, JCPL Pings from Servers;
				while(str.split(" ")[0].equals("JCPL")){
					send("REDY");
					str = recieve();
				}
				
                if(str.equals("NONE")){
                    break;
                } else {

					// Clear Arraylist of Burst Servers, Servers Returned For Every Job is Different
					burstServers.clear();

					// jobID identifies the current job ID
					String job  = str; // Making Copy of JobN
					int jobID = Integer.parseInt(str.split(" ")[2]);

					int core = Integer.parseInt(str.split(" ")[4]);
					int memory = Integer.parseInt(str.split(" ")[5]);;
					int disk = Integer.parseInt(str.split(" ")[6]);;

					// Gets all burst servers
                    send("GETS Avail "+ core + " " + memory + " " + disk);
					str = recieve();

					String flag = (str.split(" ")[1]);
					System.out.println("Flag recieved is `" + flag + " `");
					
					// If no burst servers, fetch all available servers [Capable to run job, but busy]
					if(flag.equals("0")){
						send("OK");
						str = recieve();
						send("GETS Capable "+ core + " " + memory + " " + disk);
						str = recieve();
					}
					
					//Server Count is the total number of servers available
					int serverCount = Integer.parseInt(str.split(" ")[1]);
					System.out.println("ServerCount + " + serverCount);
					send("OK");
					
					// Server Array of String Type, with Server Count Limit.
					String[] servers = new String[serverCount];	
					
					// Servers are Stored Line By Line.
					for (int i = 0; i < serverCount; i++) {
						servers[i] = recieve();
					}

					if(parseIntoBurstServers){
						for(int i = 0; i < servers.length; i++) {
								burstServers.add(servers[i]);
						}
					}

					// First available Server is Choosen. Servers are send from lowest cores to highest from one type to another.
					// Picking First One makes sure, cores is less, server rental cost is less.
					serverPointer = 0;

					send("OK");
					str = recieve();
					
					// Checks if there is a Last SCHD server Available
					if(lastSCHDServer != null){
						String ServerType =  lastSCHDServer[0];
						String ServerID =   lastSCHDServer[1] ;
						// Checks how many jobs the particular server is running
						send("CNTJ " + ServerType + " " + ServerID + " " + "2");
						str = recieve();
						int runningJobs = Integer.parseInt(str);
		/* Checks, If the present job can be run on the last SCHD server and the running processes is not more than 2.
		Minimum cores for a job is one, majority of servers have atleast two Cores. Prevents Quening of Jobs for server(Maximising Turnaround Time),
		making sure atmost two jobs are always running(Maximising Resource Utilization)
		*/
						if(LastSCHDServer_IsCapable(core, memory, disk) && runningJobs < 2){
							updateLastSCHDServerValues(core, memory, disk);
							serverToSchedule = lastSCHDServer;
						} 
					} else {
						// Fetch Server from DATA Sent
						serverToSchedule = burstServers.get(serverPointer).split(" ");
					}
					
					// Check to Make Sure Server Reply is JOBN
					if (str.equals(".") && job.split(" ")[0].equals("JOBN")) {
						send("SCHD " + jobID + " " + serverToSchedule[0] + " " + serverToSchedule[1]);
						str = recieve();

					}
				}
			}
			send("QUIT");
			String quit = inputStream.readLine();
			System.out.println(quit);
			close();
            
		} catch (Exception e) {
			System.out.println(e); // Catches any Error, Prints to Console.
		}
	}

	/*
		Utility Method to Perform Handshake with Server.
	*/
    public static void performHandshake() throws Exception{
        send("HELO");
        String str = recieve();
        if (str.equals("OK")) {
            String username = System.getProperty("user.name");
            send("AUTH " + username);
            recieve();
            System.out.println("!-- Handshake Completed --! \n");
            return;
        }
        System.out.println("!-- Could not Handshake, Check Credentials, Socket! --! \n");
    }


	/*
		Utility method to Send Message to Client.
	*/
    public static void send(String message){
		try {
			String msg = message + "\n";
			outputStream.write(msg.getBytes());
			outputStream.flush();
			System.out.println("SENT : " + msg);
			return;
		} catch (Exception e){
			System.out.println("Something Wrong");
		}
    }


	/*
		Utility Method used to Recieve message from Server.
	*/
    public static String recieve() throws IOException{
        String str = (String) inputStream.readLine();
        System.out.println("RCVD : " + str);
        return str;
    }


	/*
		Flushes Output Stream, Closes Both Streams and Client Socket.
	*/
    public static void close() {
        try {
            outputStream.flush();
            inputStream.close();
            clientSocket.close();
        } catch (Exception e){
            System.out.println(e);
        }
    }

	/*
		Utility Method used updateLastSCHDServer values;
	*/
	public static void updateLastSCHDServerValues(int core, int memory, int disk){
		int size = lastSCHDServer.length - 1;

		Integer newCore = Integer.parseInt(lastSCHDServer[size - 4]) - core;
		Integer newMemory = Integer.parseInt(lastSCHDServer[size - 3]) - memory;
		Integer newDisk = Integer.parseInt(lastSCHDServer[size - 2]) - memory;

		lastSCHDServer[size - 4] = newCore.toString();
		lastSCHDServer[size - 3] = newMemory.toString();
		lastSCHDServer[size - 2] = newDisk.toString();
	}

	/*
		Utility Method used to check if LastSCHDServer is capable of running the new JOB.
	*/
	public static boolean LastSCHDServer_IsCapable(int core, int memory, int disk){
		int size = lastSCHDServer.length - 1;
		Integer newCore = Integer.parseInt(lastSCHDServer[size - 4]) - core;
		Integer newMemory = Integer.parseInt(lastSCHDServer[size - 3]) - memory;
		Integer newDisk = Integer.parseInt(lastSCHDServer[size - 2]) - memory;
		if(newCore > 0 && newMemory > 0 && newDisk > 0){
			updateLastSCHDServerValues(newCore, newMemory, newDisk);
			return true;
		}
		return false;
	}

}
