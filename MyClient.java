import java.io.*;
import java.net.*;
import java.util.*;

public class MyClient {
    public static Socket clientSocket;
    public static DataOutputStream outputStream;
    public static BufferedReader inputStream;
    public static List<String> burstServers;
	public static String lastSHDServer;
    public static void main(String[] args){
		try {
			/*
				Initialises Neccessary Streams, Sockets and Performs Handshake with Client.
			*/
			clientSocket = new Socket("localhost", 50000);
			outputStream = new DataOutputStream(clientSocket.getOutputStream());
			inputStream = new BufferedReader(new InputStreamReader(clientSocket.getInputStream()));
            performHandshake();

		    burstServers = new ArrayList<>(); // Stores servers from largest to smallest.
			int serverPointer = 0; //Pointer for a List of Arrays with Servers
			String str = "";
			boolean parseIntoBurstServers = true;//Boolean Flag to run conditional only once

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

					burstServers.clear();
                    // JOBN 101 3 380 2 900 2500
					// jobID identifies the current job ID
					String job  = str; // Making Copy of JobN
					int jobID = Integer.parseInt(str.split(" ")[2]);

					int core = Integer.parseInt(str.split(" ")[4]);
					int memory = Integer.parseInt(str.split(" ")[5]);;
					int disk = Integer.parseInt(str.split(" ")[6]);;

                    send("GETS Avail "+ core + " " + memory + " " + disk);
					str = recieve();

					String flag = (str.split(" ")[1]);
					System.out.println("Flag recieved is `" + flag + " `");
			
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
					
					// ServersToSchedule Contains data of single largest array.
					// Values are Reassigned every iteration of while loop.
					String[] serverToSchedule = burstServers.get(serverPointer).split(" ");
					
					send("OK");
					str = recieve();
					

					// Check to Make Sure Server Reply is JOBN
					if (str.equals(".") && job.split(" ")[0].equals("JOBN")) {
						
						send("SCHD " + jobID + " " + serverToSchedule[0] + " " + serverToSchedule[1]);

						// Increase Counter
						// serverPointer++;

						str = recieve();
						
						// Reset Counter Once You hit Limit
						if(serverPointer == burstServers.size()) {
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

	public static void checkIfLastServerCapable() {

	}

}
