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
					
					//JOBN 101 3 380 2 900 2500
					// jobID identifies the current job ID
					int jobID = Integer.parseInt(str.split(" ")[2]);
					System.out.println("JOBID " + jobID);
					String job  = str; // Making Copy of JobN
					int core = Integer.parseInt(str.split(" ")[4]);
					int memory = Integer.parseInt(str.split(" ")[5]);;
					int disk = Integer.parseInt(str.split(" ")[6]);;

					
                    send("GETS Capable "+ core + " " + memory + " " + disk);
					str = recieve();	
					System.out.println(str);
					int count = Integer.parseInt(str.split(" ")[1]);
					send("OK");
					str = recieve();
					System.out.println(str);
					String serverType = (str.split(" ")[0]);
					String server = str.split(" ")[1];
					int serverID = Integer.parseInt(server);
					// SCHD jobID serverType serverID

					for(int i = 1 ; i < count ; i++){
						//To finish recieving cycle, we are only conncered with the 
						// First Entry
						recieve();
					}

					//juju 0 booting 120 0 2500 13100 1 0
					send("OK");
					str = recieve();
				

					// && job.split(" ")[0].equals("JOBN"))
					// Check to Make Sure Server Reply is JOBN
					if (str.equals(".") && job.split(" ")[0].equals("JOBN")) {
						System.out.println("SCHD" + " " + jobID + " " + serverType + " " + serverID);
						send("SCHD" + " " + jobID + " " + serverType + " " + serverID);
					}
					str = recieve();
					if(str.equals("OK")){
						continue;
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
