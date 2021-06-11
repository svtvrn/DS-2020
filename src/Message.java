import java.io.Serializable;

public class Message implements Serializable {

    private Serializable message;

    public Message(ArtistName message){
        this.message=message;
    }

    public Message(Value message){
        this.message=message;
    }

    public Message(MusicFile message){
        this.message=message;
    }

    public Message(String message){
        this.message=message;
    }

    public Message(Broker message){
        this.message=message;
    }

    public Message(Consumer message){
        this.message=message;
    }

    public Object getMessage() {
        return message;
    }
}
