import java.io.*;
import java.net.*;
import java.util.*;

public class MyClient {
    public static Socket clientSocket;
    public static DataOutputStream outputStream;
    public static BufferedReader inputStream;
    public static List<String> largestServers;
    public static void main(String[] args){
		try {
			/*
				Initialises Neccessary Streams, Sockets and Performs Handshake with Client.
			*/
			clientSocket = new Socket("localhost", 50000);
			outputStream = new DataOutputStream(clientSocket.getOutputStream());
			inputStream = new BufferedReader(new InputStreamReader(clientSocket.getInputStream()));
            performHandshake();

		    largestServers = new ArrayList<>(); // Stores servers from largest to smallest.
			int serverPointer = 0; //Pointer for a List of Arrays with Servers
			String str = "";
			boolean sortServers = true;//Boolean Flag to run conditional only once

			//Loop Keeps Running Until, None is Recieved.
			while (true) {
				
                send("REDY");
                str = recieve(); // JOBN Stored to Job.
				
		while(str.split(" ")[0].equals("JCPL")){
			send("REDY");
			str = recieve();
		}
				
                if(str.equals("NONE")){
                    break;
                } else {
		
					// jobID identifies the current job ID
					int jobID = Integer.parseInt(str.split(" ")[2]);
					String job  = str; // Making Copy of JobN

                    send("GETS All");
					str = recieve();

					//Server Count is the total number of servers available
					int serverCount = Integer.parseInt(str.split(" ")[1]);
					send("OK");
					
					// Server Array of String Type, with Server Count Limit.
					String[] servers = new String[serverCount];	
					
					// Servers are Stored Line By Line.
					for (int i = 0; i < serverCount; i++) {
						servers[i] = recieve();
					}


					if(sortServers){
						/* Identifiers for Server Cores, Name[Conditions of Largest Server] */
						int maxCore = 0; 
						String serverName = "";

						/* 
						Find Largest Server, By Iterating Entire Server List Array
						Comparing maxCore, If larger, update Server Name, Cores available. 
						*/ 

						for (int i = 0; i < servers.length; i++) {
							if(Integer.parseInt(servers[i].split(" ")[4]) > maxCore) {
								maxCore = Integer.parseInt(servers[i].split(" ")[4]);
								serverName = servers[i].split(" ")[0]; 
							}
						}

						//All Servers with Same name, Same Cores are added to the Largest Servers List.
						for(int i = 0; i < servers.length; i++) {
							if(servers[i].split(" ")[0].equals(serverName) && Integer.parseInt(servers[i].split(" ")[4]) == maxCore) {
								largestServers.add(servers[i]);
							}
						}

						//Set Flag to False;
						sortServers = false;
					}
					
					// ServersToSchedule Contains data of single largest array.
					// Values are Reassigned every iteration of while loop.
					String[] serverToSchedule = largestServers.get(serverPointer).split(" ");
					
					send("OK");
					str = recieve();
					

					// Check to Make Sure Server Reply is JOBN
					if (str.equals(".") && job.split(" ")[0].equals("JOBN")) {
						//Increase Counter
						serverPointer++;

						send("SCHD " + jobID + " " + serverToSchedule[0] + " " + serverToSchedule[1]);
						str = recieve();
						

						// Reset Counter Once You hit Limit
						if(serverPointer == largestServers.size()) {
							serverPointer = 0;
						}
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

}
