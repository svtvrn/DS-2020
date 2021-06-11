import java.io.Serializable;

public class ArtistName implements Serializable {

    private String artistName;

    public ArtistName(String artistName){
        this.artistName=artistName;
    }

    public String getName(){
        return artistName;
    }
}
