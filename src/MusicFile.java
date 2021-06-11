import java.io.Serializable;

public class MusicFile implements Serializable {

    private String trackName;
    private String artistName;
    private String albumInfo;
    private String genre;
    private byte[] musicFileExtract;

    public MusicFile(){}

    public MusicFile(String trackName, String artistName){
        this.trackName=trackName;
        this.artistName=artistName;
    }

    public MusicFile(String trackName,String artistName,String albumInfo, String genre, byte[] musicFileExtract){
        this.trackName=trackName;
        this.artistName=artistName;
        this.albumInfo=albumInfo;
        this.genre=genre;
        this.musicFileExtract=musicFileExtract;
    }

    public byte[] getMusicFileExtract(){
        return musicFileExtract;
    }

    public String getTrackName(){
        return trackName;
    }

    public String getArtistName(){
        return artistName;
    }

    public String getAlbumInfo(){
        return albumInfo;
    }

    public String getGenre(){
        return genre;
    }
}
