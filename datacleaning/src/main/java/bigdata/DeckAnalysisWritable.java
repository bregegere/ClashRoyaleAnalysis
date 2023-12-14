package bigdata;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.HashSet;

import org.apache.hadoop.io.Writable;

public class DeckAnalysisWritable implements Cloneable, Writable{
    private String deck;
    private int victories, games, clan;
    private double deltaStrength;
    private HashSet<String> players;

    DeckAnalysisWritable(){};

    DeckAnalysisWritable(String deck, int vict, int games, HashSet<String> pl, int clan, double str){
        this.deck = deck;
        this.victories = vict;
        this.games = games;
        this.players = pl;
        this.clan = clan;
        this.deltaStrength = str;
    }

    public void write(DataOutput out) throws IOException{
        out.writeUTF(deck);
        out.writeInt(players.size());
        for(String str: players){
            out.writeUTF(str);
        }
        out.writeInt(victories);
        out.writeInt(games);
        out.writeInt(clan);
        out.writeDouble(deltaStrength);
    }

    public void readFields(DataInput in) throws IOException{
        deck = in.readUTF();
        players = new HashSet<>();
        int nbPlayers = in.readInt();
        for(int i = 0; i < nbPlayers; i++){
            String player = in.readUTF();
            players.add(player);
        }
        this.victories = in.readInt();
        this.games = in.readInt();
        this.clan = in.readInt();
        this.deltaStrength = in.readDouble();
    }

    public String getDeck(){ return this.deck; }
    public int getVictories(){ return this.victories; }
    public int getGames(){ return this.games; }
    public HashSet<String> getPlayers(){ return this.players; }
    public int getPlayersLength(){ return this.players.size(); }
    public int getClan(){ return this.clan; }
    public double getDeltaStrength(){ return this.deltaStrength; }

    @Override
    public String toString(){
        StringBuilder sb = new StringBuilder();
        sb.append("{\"deck\":\"" + this.deck + "\",");
        sb.append("\"victories\":" + this.victories + ",");
        sb.append("\"games\":" + this.games + ",");
        sb.append("\"players\":" + this.players.size() + ",");
        sb.append("\"clanMax\":" + this.clan + ",");
        sb.append("\"strength\":" + this.deltaStrength + "}");
        return sb.toString();
    }

    @Override
    protected Object clone() throws CloneNotSupportedException{
        return super.clone();
    }
}
