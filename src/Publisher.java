import com.mpatric.mp3agic.ID3v1;
import com.mpatric.mp3agic.ID3v2;
import com.mpatric.mp3agic.InvalidDataException;
import com.mpatric.mp3agic.Mp3File;
import com.mpatric.mp3agic.UnsupportedTagException;
import java.io.*;
import java.math.BigInteger;
import java.net.ServerSocket;
import java.net.Socket;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.util.Collections;
import java.util.ArrayList;
import java.nio.file.Files;

public class Publisher extends Node{

    private String first;
    private String last;
    private int publisherId;
    private int port;
    private int backlog=5;

    //Sockets for the server-side operations
    Socket serverConnection=null;
    ServerSocket serverSocket =null;
    //Socket for the client-side operations
    Socket requestSocket=null;
    //Input and output streams for data
    ObjectOutputStream out=null;
    ObjectInputStream in=null;

    private ArrayList<Broker> brokers= new ArrayList<Broker>();
    private ArrayList<ArtistName> artists= new ArrayList<ArtistName>();

    public Publisher(String first,String last,int publisherId, int port){
        this.first=first;
        this.last=last;
        this.port=port;
        this.publisherId=publisherId;
    }

    private Publisher(Publisher pub){
        this.first = pub.first;
        this.last = pub.last;
        this.port=pub.port;
        this.publisherId=pub.publisherId;
        this.artists=pub.artists;
        this.brokers=pub.brokers;
    }

    public int getPort(){return port;}

    public  ArrayList<ArtistName> getArtists(){
        return artists;
    }

    //Reads file format in form of: 127.0.0.1 4234
    public void getBrokerList(){
        //File brokersFile = new File("/brokers.txt");
        BufferedReader reader=null;
        try {
            reader = new BufferedReader(new FileReader("C:\\brokers.txt"));
            String line;
            String brokerIp;
            int brokerPort;
            Broker broker;
            line = reader.readLine();
            while(line!=null){
                String[] split = line.trim().split("\\s+");
                brokerIp = split[0];
                brokerPort = Integer.parseInt(split[1]);
                broker = new Broker(brokerIp,brokerPort);
                broker.calculateKeys();
                brokers.add(broker);
                line = reader.readLine();
            }
        }catch (IOException ex){
            System.out.println("File not found");
        }finally {
            try {
                ArrayList<BigInteger> temp_id = new ArrayList<BigInteger>();
                ArrayList<Broker> sortedBrokers = new ArrayList<Broker>();
                for(Broker broker: brokers){
                    temp_id.add(broker.getBrokerId());
                }
                Collections.sort(temp_id);
                for (BigInteger id: temp_id){
                    for(Broker broker: brokers){
                        if(id.equals(broker.getBrokerId())){
                            sortedBrokers.add(broker);
                            break;
                        }
                    }
                }
                brokers = sortedBrokers;
                reader.close();
            }catch (IOException ex){
                System.out.println("Reader crashed");
            }
        }
    }

    //Returns the broker responsible for this artist(artistName).
    public Broker hashTopic(ArtistName artist) {
        //Start of MD5 hashing.
        String hash = artist.getName();
        byte[] bytesOfMessage = null;
        MessageDigest md = null;
        try {
            bytesOfMessage = hash.getBytes("UTF-8");
        } catch (UnsupportedEncodingException ex) {
            System.out.println("Unsupported encoding");
        }
        try {
            md = MessageDigest.getInstance("MD5");
        } catch (NoSuchAlgorithmException ex) {
            System.out.println("Unsupported hashing");
        }
        byte[] digest = md.digest(bytesOfMessage);
        BigInteger hashArtist = new BigInteger(digest);
        //End of MD5 hashing.
        //Check through all brokers
        if(hashArtist.compareTo(brokers.get(brokers.size()-1).getBrokerId())>0){
            hashArtist = hashArtist.mod(brokers.get(brokers.size()-1).getBrokerId());
        }
        for(int i=0; i<brokers.size(); i++){
            BigInteger id = brokers.get(i).getBrokerId();
            if(hashArtist.compareTo(id)<=0){
                Broker broker = brokers.get(i);
                return broker;
            }
        }
        return null;
    }

    //Reads the metadata of the data-set and fills the Artist array list.
    public void readMetadata(){
        File folder = new File("C:\\dataset");
        File[] listOfFiles = folder.listFiles();
        for (File file : listOfFiles) {
            if (file.isFile()) {
                //System.out.println(file.getName());
                Mp3File tagData = null;
                try {
                     tagData = new Mp3File(file);
                }catch (IOException ex){
                    System.out.println("File not found.");
                }catch (UnsupportedTagException ex){
                    System.out.println("MP3 tag data is corrupt.");
                }catch (InvalidDataException ex){
                    System.out.println("Invalid file format.");
                }
                if(tagData.hasId3v1Tag()){
                    ID3v1 tag = tagData.getId3v1Tag();
                    ArtistName artist;
                    if(tag.getArtist().equals("") || tag.getArtist()==null) {
                        tag.setArtist("Unknown Artist");
                    }
                    if(tag.getArtist().compareToIgnoreCase(first)>=0 && tag.getArtist().compareToIgnoreCase(last)<=0) {
                        artist = new ArtistName(tag.getArtist());
                        if(!artists.contains(artist)){
                            artists.add(artist);
                        }
                    }
                }else if(tagData.hasId3v2Tag()){
                    ID3v2 tag = tagData.getId3v2Tag();
                    ArtistName artist;
                    if(tag.getArtist().equals("") || tag.getArtist()==null) {
                        tag.setArtist("Unknown Artist");
                    }
                    if(tag.getArtist().compareToIgnoreCase(first)>=0 && tag.getArtist().compareToIgnoreCase(last)<=0) {
                        artist = new ArtistName(tag.getArtist());
                        if(!artists.contains(artist)){
                            artists.add(artist);
                        }
                    }
                }
            }
        }
    }

    //Reads the song requested by the consumer.
    public void readSong(ArtistName name, String trackTitle){
        Value requestedSong = null;
        File folder = new File("C:\\dataset");
        File[] listOfFiles = folder.listFiles();
        for (File file : listOfFiles) {
            if (file.isFile()) {
                Mp3File tagData;
                try {
                     tagData = new Mp3File(file);
                     if(tagData.hasId3v1Tag()){
                        ID3v1 tag = tagData.getId3v1Tag();
                        if(tag.getArtist().equals("") || tag.getArtist()==null){
                            tag.setArtist("Unknown Artist");
                        }
                        if(tag.getArtist().equalsIgnoreCase(name.getName()) && tag.getTitle().equalsIgnoreCase(trackTitle)){
                            byte[] songBuffer = Files.readAllBytes(file.toPath());
                            MusicFile data = new MusicFile(tag.getTitle(), tag.getArtist(), tag.getAlbum(), tag.getGenreDescription(), songBuffer);
                            requestedSong = new Value(data);
                            System.out.println("Song read successfully.");
                            break;
                        }
                    }else if(tagData.hasId3v2Tag()){
                        ID3v2 tag = tagData.getId3v2Tag();
                        if(tag.getArtist().equals("") || tag.getArtist()==null){
                            tag.setArtist("Unknown Artist");
                        }
                        if(tag.getArtist().equalsIgnoreCase(name.getName()) && tag.getTitle().equalsIgnoreCase(trackTitle)){
                            byte[] songBuffer = Files.readAllBytes(file.toPath());
                            MusicFile data = new MusicFile(tag.getTitle(), tag.getArtist(), tag.getAlbum(), tag.getGenreDescription(), songBuffer);
                            requestedSong = new Value(data);
                            System.out.println("Song read successfully.");
                            break;
                        }
                    }else{
                         notifyFailure(hashTopic(name));
                         return;
                     }
                }catch (IOException ex){
                    System.out.println("File not found.");
                    ex.printStackTrace();

                }catch (UnsupportedTagException ex){
                    ex.printStackTrace();
                    System.out.println("MP3 tag data is corrupt.");
                }catch (InvalidDataException ex){
                    ex.printStackTrace();
                    System.out.println("Corrupt data.");
                }

            }
        }
        push(name,requestedSong);
    }

    public void push(ArtistName artistName,Value value){
        try {
            int chunkSize=512000;
            Broker pushTo = hashTopic(artistName);
            MusicFile songInfo = value.getMusicFile();
            byte[] songBuffer = songInfo.getMusicFileExtract();
            int songLength = songBuffer.length;
            System.out.println(songLength+" bytes.");
            int j=0;
            while (j<songLength){
                MusicFile chunkInfo;
                Value chunk;
                byte[] chunkBuffer = new byte[chunkSize];
                if(j>songLength) break;
                int pos=0;
                while(pos<chunkSize){
                    if(j==songLength) break;
                    chunkBuffer[pos]=songBuffer[j];
                    pos++;
                    j++;
                }
                chunkInfo = new MusicFile(songInfo.getTrackName(),songInfo.getArtistName(),songInfo.getAlbumInfo(),songInfo.getGenre(),chunkBuffer);
                chunk = new Value(chunkInfo);
                requestSocket = new Socket("127.0.0.1",pushTo.getPort());
                out = new ObjectOutputStream(requestSocket.getOutputStream());
                out.writeObject(chunk);
                out.flush();
            }
        }catch (IOException ex) {
            System.out.println("Chunk lost");
        }
    }

    public void notifyFailure(Broker broker){
        try {
            requestSocket = new Socket("127.0.0.1", broker.getPort());
            out = new ObjectOutputStream(requestSocket.getOutputStream());
            out.writeChars("P1");
            out.flush();
        }catch (IOException ex){
            System.out.println("Notification failed.");
        }finally {
            try {
                requestSocket.close();
                out.close();
            }catch (IOException ex){
                System.out.println("Socket crashed on close.");
            }
        }
    }

    //Initialization process
    @Override
    public void init(int id){
        getBrokerList();
        readMetadata();
        try {
            for (Broker broker: brokers){
                requestSocket= new Socket("127.0.0.1",broker.getPort());
                out = new ObjectOutputStream(requestSocket.getOutputStream());
                out.writeObject(new Publisher(this));
            }
        }catch (IOException ex){
            System.out.println("Artist info wasn't transferred.");
        }
        openServer();
    }

    public void openServer(){
        try {
            serverSocket = new ServerSocket(port,backlog);
            while (true){
                System.out.println("Awaiting connection...");
                serverConnection = serverSocket.accept();
                System.out.println("Broker connected");
                PublisherHandler pubHandler = new PublisherHandler(serverConnection);
                System.out.println("Publisher handler created");
                new Thread(pubHandler).start();
            }
        }catch (IOException ex){
            ex.printStackTrace();
        } finally {
            try {
                serverSocket.close();
            } catch (IOException ex) {
                ex.printStackTrace();
            }
        }
    }

    //Begins client connection with a publisher
    @Override
    public void connect(){
        try{
            requestSocket= new Socket("localhost",port);
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
        init(publisherId);
    }

    private class PublisherHandler extends Thread{
        ObjectInputStream in;
        ObjectOutputStream out;

        public PublisherHandler(Socket connection) {
            try {
                out = new ObjectOutputStream(connection.getOutputStream());
                in = new ObjectInputStream(connection.getInputStream());
            } catch (IOException e) {
                e.printStackTrace();
            }
        }
        @Override
        public void run() {
            try{
                Object input = in.readObject();
                if(input instanceof MusicFile){
                    MusicFile musicFile = (MusicFile) input;
                    ArtistName artistName = new ArtistName(musicFile.getArtistName());
                    readSong(artistName,musicFile.getTrackName());
                }
            }catch (IOException ex){
                System.out.println("Message lost.");
            }catch (ClassNotFoundException ex){
                System.out.println("Invalid casting.");
            }
        }
    }

    public static void main(String[] arg){
        Publisher pub1 = new Publisher("A","G",1,3000);
        Publisher pub2 = new Publisher("H","P",2,3001);
        Publisher pub3 = new Publisher("Q","Z",3,3002);
        pub1.start();
        pub2.start();
        pub3.start();
    }
}
