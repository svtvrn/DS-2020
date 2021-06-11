import java.io.Serializable;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

public class Node extends Thread implements Serializable {

    static List<Broker> brokers=Collections.synchronizedList(new ArrayList<Broker>());

    public void init(int id){}

    public static List<Broker> getBrokers(){return brokers;}

    public void connect(){}

    public void disconnect(){}

    public void updateNodes(){
        brokers.add((Broker)this);
    }

    public void run(){}
}
