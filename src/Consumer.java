import java.io.IOException;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.net.Socket;
import java.util.ArrayList;
import java.util.Random;
import java.util.Scanner;

public class Consumer extends Node {

    private int consumerId;
    private Consumer copy;

    private Socket requestSocket=null;
    private ObjectOutputStream out=null;
    private ObjectInputStream in=null;

    private ArrayList<Value> downloadedSongs = new ArrayList<Value>();
    private int[] ports = {5200,5201,5202};
    private Info infoTable;
    private transient Scanner scanner = new Scanner(System.in);

    public Consumer(int consumerId){
        this.consumerId=consumerId;
    }

    private Consumer(Consumer copy){
        this.consumerId=copy.consumerId;
    }

    @Override
    public void connect(){
        try {
            //Connects to a random broker, using a random broker port
            int randomPort = ports[new Random().nextInt(ports.length)];
            requestSocket = new Socket("127.0.0.1", randomPort);
            out = new ObjectOutputStream(requestSocket.getOutputStream());
            //Action code: C1, initiates connection with the broker server.
            String command1 = "C1";
            out.writeObject(command1);
            System.out.println("Info block request");
            in = new ObjectInputStream(requestSocket.getInputStream());
            System.out.println("Waiting...");
            Object input = in.readObject();
            if(input instanceof Info) {
                infoTable = (Info) input;
                System.out.println("Info block received.");
                //infoTable.printInfo();
            }else{
                System.out.println("Info block lost.");
            }
        }catch (IOException ex){
            System.out.println("Consumer crashed");
        }catch (ClassNotFoundException ex){
            System.out.println("Invalid class.");
        }
    }

    public void register(ArtistName name){
        int brokerPort = infoTable.findBroker(name);
        System.out.println(brokerPort);
        try{
            requestSocket = new Socket("127.0.0.1",brokerPort);
            out = new ObjectOutputStream(requestSocket.getOutputStream());
            out.writeObject(copy);
            System.out.println("Registration issued.");
            System.out.println("Please enter the song: ");
            String trackTitle = scanner.nextLine().trim();
            MusicFile request = new MusicFile(trackTitle,name.getName());
            requestSocket = new Socket("127.0.0.1",brokerPort);
            out = new ObjectOutputStream(requestSocket.getOutputStream());
            out.writeObject(request);
            requestSocket = new Socket("127.0.0.1",brokerPort);
            out = new ObjectOutputStream(requestSocket.getOutputStream());
            this.sleep(2000);
            out.writeObject("C2");
            in = new ObjectInputStream(requestSocket.getInputStream());
            while (true){
               Object input = in.readObject();
                if(input instanceof Value) {
                    Value chunk = (Value) input;
                    playData(name,chunk);
                }
            }
        }catch (IOException ex) {
            System.out.println("Bad Info.");
            ex.printStackTrace();
        }catch (ClassNotFoundException ex){
            ex.printStackTrace();
        }catch (InterruptedException rc){
            rc.printStackTrace();
        }
    }

    public void disconnect(Broker broker, ArtistName name){
        broker.registeredUsers.remove(copy);
        disconnect();
        register(name);
    }

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

    public void playData(ArtistName name,Value value){
        if(value!=null) {
            System.out.println("Playing, " + value.getMusicFile().getTrackName() + " by " + name.getName());
        }
    }

    public void init(){
        this.copy = new Consumer(this);
        connect();
    }

    public void run(){
        init();
        System.out.println("Please enter an artist: ");
        String artistName = scanner.nextLine();
        ArtistName artist = new ArtistName(artistName);
        register(artist);
    }

    public static void main(String[] arg){
        Consumer con1 = new Consumer(1);
        con1.start();
    }
}
