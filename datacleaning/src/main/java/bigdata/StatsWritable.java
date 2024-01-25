package bigdata;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.HashSet;

import org.apache.hadoop.io.Writable;

public class StatsWritable implements Cloneable, Writable{
    public String cards;
    public int games;
    public int victories;
    public int clan;
    public double strength;
    public HashSet<String> players;

    StatsWritable(){}

    StatsWritable(String cards, int games, int victories, int clan, HashSet<String> players, double strength){
        this.cards = cards;
        this.games = games;
        this.victories = victories;
        this.clan = clan;
        this.strength = strength;
        this.players = players;
    }

    public void write(DataOutput out) throws IOException{
        out.writeUTF(cards);
        out.writeInt(players.size());
        for(String player: players){
            out.writeUTF(player);
        }
        out.writeInt(victories);
        out.writeInt(games);
        out.writeInt(clan);
        out.writeDouble(strength);
    }

    public void readFields(DataInput in) throws IOException{
        cards = in.readUTF();
        int size = in.readInt();
        players = new HashSet<String>();
        for(int i = 0; i < size; i++){
            String player = in.readUTF();
            players.add(player);
        }
        victories = in.readInt();
        games = in.readInt();
        clan = in.readInt();
        strength = in.readDouble();
    }

    @Override
    public String toString(){
        StringBuilder sb = new StringBuilder();
        sb.append("\"cards\":" + this.cards + ",");
        sb.append("\"victories\":" + this.victories + ",");
        sb.append("\"games\":" + this.games + ",");
        sb.append("\"players\":" + this.players.size() + ",{");
        for(String player: players){
            sb.append(player + ";");
        }
        sb.append("},");
        sb.append("\"clanMax\":" + this.clan + ",");
        sb.append("\"strength\":" + this.strength + "}");
        return sb.toString();
    }

    public String text(){
        StringBuilder sb = new StringBuilder();
        sb.append(this.victories + " ");
        sb.append(this.games + " ");
        sb.append(this.players.size() + " {");
        for(String player: players){
            sb.append(player + ";");
        }
        sb.append("} ");
        sb.append(this.clan + " ");
        sb.append(this.strength);
        return sb.toString();
    }

    public StatsWritable clone(){
        StatsWritable stats = new StatsWritable();
        stats.cards = this.cards;
        stats.games = this.games;
        stats.victories = this.victories;
        stats.clan = this.clan;
        stats.strength = this.strength;
        stats.players = new HashSet<String>();
        for(String player: this.players){
            stats.players.add(player);
        }
        return stats;
    }


}
