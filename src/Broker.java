import java.io.UnsupportedEncodingException;
import java.math.BigInteger;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.util.*;
import java.io.*;
import java.net.*;

public class Broker extends Node {

    private String ip;
    private int port;
    private BigInteger brokerId;
    private int backlog;
    //Sockets for the server-side operations
    transient Socket serverConnection=null;
    transient ServerSocket serverSocket;
    //Socket for the client-side operations
    Socket requestSocket=null;
    //Input and output streams for data
    ObjectOutputStream out=null;
    ObjectInputStream in=null;

    //Constructs broker and adds it to the list of brokers
    public Broker(String ip, int port){
        this.ip=ip;
        this.port=port;
        backlog=5;
    }

    List<Consumer> registeredUsers= new ArrayList<Consumer>();
    List<Publisher> registeredPublishers= new ArrayList<Publisher>();
    LinkedList<Value> songQueue= new LinkedList<Value>();
    ArrayList<ArtistName> artists= new ArrayList<ArtistName>();

    //Returns the broker id, which is the hash code
    public BigInteger getBrokerId(){
        return brokerId;
    }

    public int getPort(){return port;}

    //Calculates the hash value of the broker
    public void calculateKeys(){
        //Start of hashing of ip+port
        String hash = ip+Integer.toString(port);
        byte[] bytesOfMessage=null;
        MessageDigest md=null;
        try {
            bytesOfMessage = hash.getBytes("UTF-8");
        }catch (UnsupportedEncodingException ex){
            System.out.println("Unsupported encoding");
        }
        try {
            md = MessageDigest.getInstance("MD5");
        }catch (NoSuchAlgorithmException ex){
            System.out.println("Unsupported hashing");
        }
        byte[] digest = md.digest(bytesOfMessage);
        brokerId = new BigInteger(digest);
        //End of hashing of ip+port
    }

    public Publisher acceptConnection(Publisher pub){
        for(Publisher publisher : registeredPublishers){
            if(publisher.getPort()==pub.getPort()) return null;
        }
        registeredPublishers.add(pub);
        System.out.println(brokerId+ " Publisher registered successfully.");
        return pub;
    }

    public Consumer acceptConnection(Consumer con){
        registeredUsers.add(con);
        System.out.println("Consumer registered successfully.");
        return con;
    }

    public void notifyPublisher(String notification){
        requestSocket=null;
        out = null;
        try{
            requestSocket= new Socket(ip,port);
            out = new ObjectOutputStream(requestSocket.getOutputStream());
            out.writeChars(notification);
            out.flush();
            System.out.println("Notification sent");
        }catch(IOException ex) {
            System.out.println("Notification failed");
        }finally {
           disconnect();
        }
    }

    //Input stream that gets the music files
    public void pull(MusicFile request){
        try {
            int pubPort=0;
            for(Publisher pub : registeredPublishers){
                for(ArtistName artist: pub.getArtists()){
                    if(artist.getName().equalsIgnoreCase(request.getArtistName())){
                        pubPort = pub.getPort();
                        break;
                    }
                }
            }
            if(pubPort==0){
                System.out.println("This artist doesn't exist");
                return;
            }
            requestSocket = new Socket("127.0.0.1",pubPort);
            out= new ObjectOutputStream(requestSocket.getOutputStream());
            out.writeObject(request);
            System.out.println("Request sent to publisher.");
        }catch (IOException ex) {
            System.out.println("Opening stream failed");
            ex.printStackTrace();
        }
    }

    @Override
    public void init(int id){
        calculateKeys();
        updateNodes();
        openServer();
    }

    public void openServer(){
        try {
            System.out.println(port+" "+backlog);
            serverSocket = new ServerSocket(port,backlog);
            while (true){
                System.out.println("Awaiting connection...");
                serverConnection = serverSocket.accept();
                System.out.println("Client connected");
                ActionHandler actionHandler = new ActionHandler(this,serverConnection);
                System.out.println("Broker handler created");
                //actionHandler.start();
                new Thread(actionHandler).start();
            }
        }catch (IOException ex){
            System.out.println("Server crashed.");
            ex.printStackTrace();
        } finally {
            try {
                serverSocket.close();
            } catch (IOException ioException) {
                ioException.printStackTrace();
            }
        }
    }

    //Begins client connection with a publisher
    @Override
    public void connect() {
        try{
            requestSocket= new Socket(ip,port);
            out = new ObjectOutputStream(requestSocket.getOutputStream());
            out.writeChars("Connection established");
            out.flush();
        }catch(IOException ex) {
            System.out.println("Connection failed");
        }
    }

    //Ends client connection with a publisher
    @Override
    public void disconnect(){
        try {
            out.close();
            in.close();
            requestSocket.close();
        } catch (IOException ioException) {
            ioException.printStackTrace();
        }
    }

    public void run(){
        init(port);

        System.out.println("Broker thread created: "+port);
    }

    private class ActionHandler extends Thread{
        ObjectInputStream in;
        ObjectOutputStream out;
        Broker broker;

        public ActionHandler(Broker broker,Socket connection) {
            try {
                out = new ObjectOutputStream(connection.getOutputStream());
                in = new ObjectInputStream(connection.getInputStream());
                this.broker=broker;
            } catch (IOException e) {
                e.printStackTrace();
            }
        }

        @Override
        public void run() {
            try {
                Object input = in.readObject();
                if (input instanceof String) {
                    String command = (String) input;
                    //Consumer Action 1: Retrieves info file.
                    if (command.equals("C1")) {
                        Info infoTable = new Info();
                        infoTable.setBrokerList(getBrokers());
                        infoTable.setArtistTable();
                        out.writeObject(infoTable);
                    }else if (command.equals("P1")) {
                        System.out.println("Name added");
                        out.writeObject("Song was not found.");
                    }else if(command.equals("C2")){
                        if(!broker.songQueue.isEmpty()){
                            for (Value chunk: songQueue){
                                chunk = songQueue.getFirst();
                                out.writeObject(chunk);
                                out.flush();
                                this.sleep(1000); }
                        }
                    }
                }else if(input instanceof MusicFile) {
                    MusicFile request = (MusicFile) input;
                    pull(request);
                }else if(input instanceof Value){
                    Value chunk = (Value)input;
                    broker.songQueue.add(chunk);
                }else if(input instanceof Consumer) {
                    acceptConnection((Consumer)input);
                }else if(input instanceof Publisher) {
                    Publisher pub = (Publisher) input;
                    for (ArtistName artist : pub.getArtists()) {
                        int port = pub.hashTopic(artist).getPort();
                        if (broker.getPort() == port) {
                            if(!broker.artists.contains(artist)) {
                                System.out.println(brokerId+" "+artist.getName());
                                broker.artists.add(artist);
                            }
                            acceptConnection(pub);
                        }
                    }
                }
            }catch (IOException ex){
                ex.printStackTrace();
            }catch (ClassNotFoundException ex){
                System.out.println("Invalid casting.");
            }catch (InterruptedException ex){
                ex.printStackTrace();
            }
        }
    }

    public static void main(String[] arg){
        Broker bro1 = new Broker("127.0.0.1",5200);
        Broker bro2 = new Broker("127.0.0.1",5201);
        Broker bro3 = new Broker("127.0.0.1",5202);
        bro1.start();
        bro2.start();
        bro3.start();
    }
}