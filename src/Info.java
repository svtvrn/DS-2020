import java.io.Serializable;
import java.math.BigInteger;
import java.util.ArrayList;
import java.util.List;
import java.util.HashMap;

public class Info implements Serializable {

    private List<Broker> brokerList = new ArrayList<Broker>();
    private HashMap<BigInteger,ArrayList<ArtistName>> artistTable = new HashMap<BigInteger,ArrayList<ArtistName>>();

    public Info(){}

    public void setBrokerList(List<Broker> brokerList) {
        this.brokerList=brokerList;
    }

    public void setArtistTable(){
        for(Broker broker: brokerList){
            artistTable.put(broker.getBrokerId(),broker.artists);
        }
    }

    public int findBroker(ArtistName artistName){
        int port;
        for(Broker broker: brokerList){
            ArrayList<ArtistName> temp_list = artistTable.get(broker.getBrokerId());
            for(ArtistName artist : temp_list){
                if(artist.getName().equalsIgnoreCase(artistName.getName())){
                    port=broker.getPort();
                    return port;
                }
            }
        }
        return 0;
    }

    public void printInfo(){
        for(Broker broker: brokerList) {
            ArrayList<ArtistName> temp_list = artistTable.get(broker.getBrokerId());
            for (ArtistName artist : temp_list) {
                System.out.println(broker.getBrokerId()+" "+artist.getName());
            }
        }
    }
}
