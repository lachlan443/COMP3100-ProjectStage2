import java.net.*;
import java.io.*;
import java.util.ArrayList;

public class MyClient {
    public static void main(String args[]) {
        // arguments supply message and hostname of destination
        Socket s = null;
        try {
            int serverPort = 50000;
            s = new Socket("127.0.0.1", serverPort);
            BufferedReader in = new BufferedReader(new InputStreamReader(s.getInputStream()));
            DataOutputStream out = new DataOutputStream(s.getOutputStream());
            String data;

            //Start of the 3 way handshake, client sends HELO
            out.write(("HELO\n").getBytes());
            out.flush();
            data = in.readLine();
            //Server responds OK, client sends AUTH.
            if (data.equals("OK")) {
                out.write(("AUTH "+System.getProperty("user.name")+"\n").getBytes());
                out.flush();
                data = in.readLine();
            }


            //For each job.
            while (true) {
                out.write(("REDY\n").getBytes());
                out.flush();
                data = in.readLine();
                String[] request = data.split("\\s"); //Split the response by the whitespaces.

                if (data.equals("NONE")) {
                    break;
                }
                //Response is either JCPL or JOBN
                
            
                if (request[0].equals("JOBN")) {
                    Job jobn = new Job(request); 
                    
                    // Try GETS Avail First, if nothing is avaliable,
                    // then use GETS Capable
                    out.write(("GETS Avail "+jobn.getCore()+" "+jobn.getMemory()+" "+jobn.getDisk()+"\n").getBytes());
                    out.flush();

                    //Recieve DATA, then send OK.
                    data = in.readLine();
                    out.write(("OK\n").getBytes());
                    out.flush();    //msgSplit = {DATA nRecs recLen}
                    String msgSplit[] = data.split("\\s");  //Split string by whitespaces
                    int nRecs = Integer.parseInt(msgSplit[1]);
                    Server selected = new Server();


                    // If no servers, recieve a dot, then continue to gets Capable
                    if (nRecs == 0) {
                        // Should recieve a dot from OK
                        data = in.readLine();
                        
                        out.write(("GETS Capable "+jobn.getCore()+" "+jobn.getMemory()+" "+jobn.getDisk()+"\n").getBytes());
                        out.flush();

                        // Read in new data from gets capable, and send OK
                        data = in.readLine();
                        msgSplit = data.split("\\s"); //msgSplit = {DATA nRecs recLen}
                        nRecs = Integer.parseInt(msgSplit[1]);
                        out.write(("OK\n").getBytes());
                        out.flush();


                        // Create an ArrayList of all the servers.
                        ArrayList<Server> servers = new ArrayList<>();
                        for (int i = 0; i < nRecs; i++) {
                            data = in.readLine();
                            Server curr = new Server(data);
                            servers.add(curr);
                        }

                        // Schedule to the server with the least waiting jobs.
                        // If all servers have same number of waitingJobs, 
                        // then schedule to least powerful.
                        boolean foundActiveServer = false;
                        for(int i = 0; i < servers.size(); i++) {
                            // Check for any server that isnt already active.
                            if (!servers.get(i).getState().equals("active")) {
                                if(servers.get(i).getState().equals("unavaliable")) {
                                    continue;  // If server is unavaliable, then move on.
                                }
                                selected = servers.get(i);
                                foundActiveServer = true;
                                break;
                            }
                        }

                        // All servers from GETS Capable are active.
                        // So we should schedule to the first 
                        // server with lowest waiting job count
                        if (!foundActiveServer) {
                            Server lowestCount = servers.get(servers.size()-1);

                            // Traverse the list backwards, we want the first LowestCount.
                            for (int i = servers.size()-1; i >= 0; i--) {
                                if (servers.get(i).getTotalJobs() < lowestCount.getTotalJobs()) {
                                    lowestCount = servers.get(i);
                                }
                            }
                            selected = lowestCount;
                        }
                    } else {
                        //If there are recs from avail, then simply read the first one and then schedule.
                        for (int i = 0; i < nRecs; i++) {
                            data = in.readLine();
                            // System.out.println("Recieved: " + data);

                            if(i == 0) {
                                // System.out.println("Selected first server: continuing to read lines");
                                Server temp = new Server(data);
                                selected = temp;
                            }
                        }
                    }
                   
                    //Send OK, recieve ., then schedule
                    out.write(("OK\n").getBytes());
                    out.flush();
                    data = in.readLine();
                    out.write(("SCHD "+jobn.getID()+" "+selected.getType()+" "+selected.getID()+"\n").getBytes());
                    out.flush(); //SCHD jobID serverType serverID

                    //Wait for response from server.
                    while (data == in.readLine()) {
                        continue;
                    }
                }
            }
            //Send quit as the final step.
            out.write(("QUIT\n").getBytes());
            out.flush();

        } catch (UnknownHostException e) {
            System.out.println("Sock:" + e.getMessage());
        } catch (EOFException e) {
            System.out.println("EOF:" + e.getMessage());
        } catch (IOException e) {
            System.out.println("IO:" + e.getMessage());
        } finally {
            if (s != null) try {
                s.close(); //Close the socket
            } catch (IOException e) {
                System.out.println("close:" + e.getMessage());
            }
        }
    }
}

class Job {
    int jobID;
    int core;
    int memory;
    int disk;

    public Job(String[] request) {
        //Request = {JOBN submitTime jobID estRuntime core memory disk}
        jobID = Integer.parseInt(request[2]);
        core = Integer.parseInt(request[4]);
        memory = Integer.parseInt(request[5]);
        disk = Integer.parseInt(request[6]);
    }

    public int getID() {return jobID;}
    public int getCore() {return core;}
    public int getMemory() {return memory;}
    public int getDisk() {return disk;}
}

class Server {
    String serverType;
    int serverID;
    String state;
    // int curStartTime;
    int core;
    int memory;
    int disk;
    int waitingJobs;
    int runningJobs;
    int totalJobs;

    public Server() {}

    public Server(String serv) {
        //message[] = {ServerType serverID state curStartTime core memory disk #wJobs #rJobs}
        String message[] = serv.split("\\s");

        this.serverType = message[0];
        this.serverID = Integer.parseInt(message[1]);
        this.state = message[2];
        // this.curStartTime = Integer.parseInt(message[3]);
        this.core = Integer.parseInt(message[4]);
        this.memory = Integer.parseInt(message[5]);
        this.disk = Integer.parseInt(message[6]);
        this.waitingJobs = Integer.parseInt(message[7]);
        this.runningJobs = Integer.parseInt(message[8]);
        this.totalJobs = waitingJobs + runningJobs;
    }

    public String getType() {return serverType;}
    public int getID() {return serverID;}
    public String getState() { return state;}
    public int getCore() {return core;}
    public int getMemory() {return memory;}
    public int getDisk() {return disk;}
    public int getTotalJobs() {return totalJobs;}

    public void setTotalJobs(int w) {this.totalJobs = w;}
}
