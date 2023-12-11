package bigdata;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.TreeSet;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.lib.input.SequenceFileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.SequenceFileOutputFormat;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.json.simple.JSONObject;
import org.json.simple.JSONValue;

public class DeckTopK {

    protected static Comparator ratio = new Comparator<DeckAnalysisWritable>() {
            public int compare(DeckAnalysisWritable deck1, DeckAnalysisWritable deck2){
                return Double.compare(((double) deck1.getVictories()) / deck1.getGames(), ((double) deck2.getVictories()) / deck2.getGames());
            }
    };

    protected static  Comparator victories = new Comparator<DeckAnalysisWritable>() {
            public int compare(DeckAnalysisWritable deck1, DeckAnalysisWritable deck2){
                return Integer.compare(deck1.getVictories(), deck2.getVictories());
            }
    };

    protected static Comparator games = new Comparator<DeckAnalysisWritable>() {
            public int compare(DeckAnalysisWritable deck1, DeckAnalysisWritable deck2){
                return Integer.compare(deck1.getGames(), deck2.getGames());
            }
    };

    protected static Comparator players = new Comparator<DeckAnalysisWritable>() {
            public int compare(DeckAnalysisWritable deck1, DeckAnalysisWritable deck2){
                return Integer.compare(deck1.getPlayers().size(), deck2.getGames().size());
            }
    };

    protected static Comparator clanMax = new Comparator<DeckAnalysisWritable>() {
            public int compare(DeckAnalysisWritable deck1, DeckAnalysisWritable deck2){
                return Integer.compare(deck1.getClan(), deck2.getClan());
            }
    };

    protected static Comparator strength = new Comparator<DeckAnalysisWritable>() {
            public int compare(DeckAnalysisWritable deck1, DeckAnalysisWritable deck2){
                return Double.compare((-1 * deck1.getStrength()), -1 * deck2.getStrength());
            }
    };


    public static class TopKMapper extends Mapper<Text, DeckAnalysisWritable, NullWritable, DeckAnalysisWritable>{

        private int k;
        private Comparator comp;

        private TreeSet<DeckAnalysisWritable> decks;

        

        @Override
        protected void setup(Context context){
            Configuration conf = context.getConfiguration();
            k = conf.getInt("k", 100);
            String comparator = conf.get("comparator", "ratio");
            switch(comparator){
                case "ratio":
                    comp = DeckTopK.ratio;
                    break;
                case "victories":
                    comp = DeckTopK.victories;
                    break;
                case "games":
                    comp = DeckTopK.games;
                    break;
                case "players":
                    comp = DeckTopK.players;
                    break;
                case "clanMax":
                    comp = DeckTopK.clanMax;
                    break;
                case "strength":
                    comp = DeckTopK.strength;
                    break;
            }
            decks = new TreeSet<>(comp);
        }

        @Override
        public void map(Text key, DeckAnalysisWritable value, Context context) throws IOException, InterruptedException{
            if(value.getGames() < 10) return;
            try{
                DeckAnalysisWritable deckClone = (DeckAnalysisWritable) value.clone();
                decks.add(deckClone);
                if(decks.size() > k) decks.remove(decks.first());
            } catch (CloneNotSupportedException e){
                context.getCounter("debug", "clone").increment(1);
            }
        }

        @Override
        protected void cleanup(Context context) throws IOException, InterruptedException{
            for(DeckAnalysisWritable deck: decks){
                context.write(NullWritable.get(), deck);
            }
        }
    }

    public static class TopKReducer extends Reducer<NullWritable, DeckAnalysisWritable, Text, DeckAnalysisWritable>{

        private int k;
        private Comparator comp;
        private TreeSet<DeckAnalysisWritable> decks;

        @Override
        protected void setup(Context context){
            Configuration conf = context.getConfiguration();
            k = conf.getInt("k", 100);
            String comparator = conf.get("comparator", "ratio");
            switch(comparator){
                case "ratio":
                    comp = DeckTopK.ratio;
                    break;
                case "victories":
                    comp = DeckTopK.victories;
                    break;
                case "games":
                    comp = DeckTopK.games;
                    break;
                case "players":
                    comp = DeckTopK.players;
                    break;
                case "clanMax":
                    comp = DeckTopK.clanMax;
                    break;
                case "strength":
                    comp = DeckTopK.strength;
                    break;
            }
            decks = new TreeSet<>(comp);
        }

        @Override
        public void reduce(NullWritable key, Iterable<DeckAnalysisWritable> values, Context context) throws IOException, InterruptedException{
            for(DeckAnalysisWritable deck: values){
                try{
                    DeckAnalysisWritable deckClone = (DeckAnalysisWritable) deck.clone();   
                    decks.add(deckClone);
                    if(decks.size() > k) decks.remove(decks.first());
                } catch (CloneNotSupportedException e){
                    context.getCounter("debug", "clone").increment(1);
                }
            }
            for(DeckAnalysisWritable deck: decks.descendingSet()){
                context.write(new Text(deck.getDeck()), deck);
            }
        }
    }

    public static void main(String[] args) throws Exception {
        Configuration conf = new Configuration();
        int k;
        String comparator;
        try{
            k = Integer.parseInt(args[2]);
            comparator = args[3];
        } catch (Exception e){
            k = 100;
            comparator = "ratio";
        }
        conf.setInt("k", k);
        conf.set("comparator", comparator);
        Job job = Job.getInstance(conf, "DeckTopK");
        job.setNumReduceTasks(1);
        job.setJarByClass(DeckTopK.class);
        job.setMapperClass(TopKMapper.class);
        job.setMapOutputKeyClass(NullWritable.class);
        job.setMapOutputValueClass(DeckAnalysisWritable.class);
        job.setReducerClass(TopKReducer.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(DeckAnalysisWritable.class);
        job.setOutputFormatClass(TextOutputFormat.class);
        job.setInputFormatClass(SequenceFileInputFormat.class);
        FileInputFormat.addInputPath(job, new Path(args[0]));
        FileOutputFormat.setOutputPath(job, new Path(args[1]));
        System.exit(job.waitForCompletion(true) ? 0 : 1);
    }
}