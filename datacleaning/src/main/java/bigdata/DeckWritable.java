package bigdata;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

import org.apache.hadoop.io.Writable;

public class DeckWritable implements Writable {
    private String cards;
    private Double strength;

    DeckWritable(){}

    DeckWritable(String cards, Double str){
        this.cards = cards;
        this.strength = str;
    } 

    public void write(DataOutput out) throws IOException {
        out.writeUTF(this.cards);
        out.writeDouble(this.strength);
    }

    public void readFields(DataInput in) throws IOException {
        this.cards = in.readUTF();
        this.strength = in.readDouble();
    }

    public String getCards(){
        return this.cards;
    }

    public Double getStrength(){
        return this.strength;
    }

}