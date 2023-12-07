package bigdata;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

import org.apache.hadoop.io.Writable;

public class PlayerWritable implements Writable {
    private String id;
    private DeckWritable deck;
    private int clanTrophies;

    PlayerWritable(){}

    PlayerWritable(String id, DeckWritable deck, int clan){
        this.id = id;
        this.deck = deck;
        this.clanTrophies = clan;
    }

    public void write(DataOutput out) throws IOException{
        out.writeUTF(this.id);
        deck.write(out);
        out.writeInt(this.clanTrophies);
    }

    public void readFields(DataInput in) throws IOException{
        this.id = in.readUTF();
        this.deck = new DeckWritable();
        this.deck.readFields(in);
        this.clanTrophies = in.readInt();
    }

    public String getPlayerId(){
        return this.id;
    }

    public DeckWritable getDeck(){
        return this.deck;
    }

    public int getClanTrophies(){
        return this.clanTrophies;
    }
}