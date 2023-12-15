package bigdata;

import java.io.IOException;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Calendar;
import java.util.Date;
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
            SimpleDateFormat dateFormat = new SimpleDateFormat("yyyy-MM-dd");
            HashSet<String> players = new HashSet();
            int[] weekVictories = new int[53]; int[] weekGames = new int[53]; int[] weekClanMax = new int[53];
            double[] weekStrength = new double[53];
            int victories = 0;
            int games = 0;
            int clanMax = 0;
            double strength = 0;

            
            for(GameWritable game: values){
                String day = game.getDate().split("T")[0];
                Date date;
                try{
                    date = dateFormat.parse(day);
                } catch (Exception e){
                    return;
                }
                Calendar cal = Calendar.getInstance();
                cal.setTime(date);
                //first week == 1
                int week = cal.WEEK_OF_YEAR - 1;
                //first month == 0
                int month = cal.MONTH;

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
                    
                    if(winner.getClanTrophies() > weekClanMax[week]) weekClanMax[week] = winner.getClanTrophies();
                    weekVictories[week]++;
                    weekStrength[week] += (winner.getDeck().getStrength() - loser.getDeck().getStrength());
                    weekGames[week] += 2;
                } else {
                    if(winner.getDeck().getCards().equals(key.toString())){
                        players.add(winner.getPlayerId());
                        victories++;
                        strength += (winner.getDeck().getStrength() - loser.getDeck().getStrength());
                        if(winner.getClanTrophies() > clanMax) clanMax = winner.getClanTrophies();

                        weekVictories[week]++;
                        weekStrength[week] += (winner.getDeck().getStrength() - loser.getDeck().getStrength());
                        if(winner.getClanTrophies() > weekClanMax[week]) weekClanMax[week] = winner.getClanTrophies();
                    }  else {
                        players.add(loser.getPlayerId());
                    }
                    games++;
                    weekGames[week]++;
                }
            }
            if(games >= 10){
                double meanStrength = (Double) strength / games;
                DeckAnalysisWritable daw = new DeckAnalysisWritable(key.toString(), victories, games, players.size(), clanMax, meanStrength);
                context.write(key, daw);
            }
            for(int i = 0; i < 53; i++){
                if(weekGames[i] >= 5){
                    double meanStrength = (Double) weekStrength[i] / weekGames[i];
                    DeckAnalysisWritable daw = new DeckAnalysisWritable(key.toString(), weekVictories[i], weekGames[i], players.size(), weekClanMax[i], meanStrength);
                    Text weekKey = new Text(i + " " + key.toString());
                    context.write(weekKey, daw);
                }
            }
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
        job.setOutputFormatClass(TextOutputFormat.class);
        job.setInputFormatClass(SequenceFileInputFormat.class);
        FileInputFormat.addInputPath(job, new Path(args[0]));
        FileOutputFormat.setOutputPath(job, new Path(args[1]));
        System.exit(job.waitForCompletion(true) ? 0 : 1);
    }
}