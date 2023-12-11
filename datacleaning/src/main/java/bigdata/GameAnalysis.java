package bigdata;

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

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

public class GameAnalysis {
    public static class AnalysisMapper extends Mapper<NullWritable, GameWritable, Text, GameWritable>{

        @Override
        public void map(NullWritable key, GameWritable value, Context context) throws IOException, InterruptedException{
            Text deck1 = new Text(value.getPlayerOne().getDeck().getCards());
            Text deck2 = new Text(value.getPlayerTwo().getDeck().getCards());

            if(deck1.toString().equals(deck2.toString())){
                context.write(deck1, value);
            } else {
                context.write(deck1, value);
                context.write(deck2, value);
            }
            
        }
    }

    public static class AnalysisReducer extends Reducer<Text, GameWritable, Text, DeckAnalysisWritable>{



        /*@Override
        protected void setup(org.apache.hadoop.mapreduce.Mapper.Context context){}*/

        @Override
        public void reduce(Text key, Iterable<GameWritable> values, Context context) throws IOException, InterruptedException{
            HashSet<String> players = new HashSet();
            int victories = 0;
            int games = 0;
            int clanMax = 0;
            double strength = 0;

            
            for(GameWritable game: values){
                PlayerWritable winner, loser;
                if(game.getCrownOne() > game.getCrownTwo()){
                    winner = game.getPlayerOne();
                    loser = game.getPlayerTwo();
                } else {
                    winner = game.getPlayerTwo();
                    loser = game.getPlayerOne();
                }

                if(game.getPlayerOne().getDeck().getCards().equals(game.getPlayerTwo().getDeck().getCards())){
                    players.add(winner.getPlayerId());
                    players.add(loser.getPlayerId());
                    if(winner.getClanTrophies() > clanMax) clanMax = winner.getClanTrophies();
                    victories++;
                    strength += (winner.getDeck().getStrength() - loser.getDeck().getStrength());
                    games += 2;
                } else {
                    if(winner.getDeck().getCards().equals(key.toString())){
                        players.add(winner.getPlayerId());
                        victories++;
                        strength += (winner.getDeck().getStrength() - loser.getDeck().getStrength());
                        if(winner.getClanTrophies() > clanMax) clanMax = winner.getClanTrophies();
                    }  else {
                        players.add(loser.getPlayerId());
                    }
                    games++;
                }
            }
            double meanStrength = (Double) strength / games;
            DeckAnalysisWritable daw = new DeckAnalysisWritable(key.toString(), victories, games, players, clanMax, meanStrength);
            context.write(key, daw);
        }
    }

    public static void main(String[] args) throws Exception {
        Configuration conf = new Configuration();
        Job job = Job.getInstance(conf, "CRGameAnalysis");
        job.setNumReduceTasks(1);
        job.setJarByClass(GameAnalysis.class);
        job.setMapperClass(AnalysisMapper.class);
        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(GameWritable.class);
        job.setReducerClass(AnalysisReducer.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(DeckAnalysisWritable.class);
        job.setOutputFormatClass(SequenceFileOutputFormat.class);
        job.setInputFormatClass(SequenceFileInputFormat.class);
        FileInputFormat.addInputPath(job, new Path(args[0]));
        FileOutputFormat.setOutputPath(job, new Path(args[1]));
        System.exit(job.waitForCompletion(true) ? 0 : 1);
    }
}