package bigdata;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

import org.apache.hadoop.io.Writable;

public class GameWritable implements Writable {
    private String date;
    private PlayerWritable player1;
    private PlayerWritable player2;
    private int crown1;
    private int crown2;

    GameWritable(){}

    GameWritable(String date, PlayerWritable p1, PlayerWritable p2, int crown1, int crown2){
        this.date = date;
        this.player1 = p1;
        this.player2 = p2;
        this.crown1 = crown1;
        this.crown2 = crown2;
    }

    public void write(DataOutput out){
        out.writeUTF(date);
        player1.write(out);
        player2.write(out);
        out.writeInt(crown1);
        out.writeInt(crown2);
    }

    public void readFields(DataInput in){
        this.date = in.readUTF();
        this.player1 = new PlayerWritable(); this.player1.readFields(in);
        this.player2 = new PlayerWritable(); this.player2.readFields(in);
        this.crown1 = in.readInt();
        this.crown2 = in.readInt();
    }

    public String getDate(){
        return this.date;
    }

    public PlayerWritable getPlayerOne(){
        return this.player1;
    }
    
    public PlayerWritable getPlayerTwo(){
        return this.player2;
    }

    public int getCrownOne(){
        return this.crown1;
    }

    public int getCrownTwo(){
        return this.crown2;
    }
}