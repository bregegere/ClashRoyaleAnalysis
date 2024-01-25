package bigdata;

import java.io.IOException;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Calendar;
import java.util.Date;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Set;
import java.util.stream.*;

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
    public static class AnalysisMapper extends Mapper<NullWritable, GameWritable, Text, StatsWritable>{

        private SimpleDateFormat dateFormat = new SimpleDateFormat("yyyy-MM-dd");

        @Override
        public void map(NullWritable key, GameWritable value, Context context) throws IOException, InterruptedException{            
            Text deck1 = new Text(value.getPlayerOne().getDeck().getCards());
            Text deck2 = new Text(value.getPlayerTwo().getDeck().getCards());
            String orderedDeck1 = orderCards(deck1.toString());
            String orderedDeck2 = orderCards(deck2.toString());

            String day = value.getDate().split("T")[0];
            Date date;
            try{
                date = dateFormat.parse(day);
            } catch (Exception e){
                return;
            }
            Calendar cal = Calendar.getInstance();
            cal.setTime(date);

            //first week == 1
            int week = cal.get(cal.WEEK_OF_YEAR) - 1;
            //first month == 0
            int month = cal.get(cal.MONTH);

            int clan = (value.getCrownOne() > value.getCrownTwo() ? value.getPlayerOne().getClanTrophies() : value.getPlayerTwo().getClanTrophies());
            double strength = (value.getCrownOne() > value.getCrownTwo() ? 
            value.getPlayerOne().getDeck().getStrength() - value.getPlayerTwo().getDeck().getStrength()
            : value.getPlayerTwo().getDeck().getStrength() - value.getPlayerOne().getDeck().getStrength());

            if(orderedDeck1.equals(orderedDeck2)){
                HashSet<String> players = new HashSet<>();
                players.add(value.getPlayerOne().getPlayerId());
                players.add(value.getPlayerTwo().getPlayerId());

                String newDeckId = orderedDeck1;

                context.write(new Text(newDeckId), new StatsWritable(newDeckId, 2, 1, clan, players, strength));
                context.write(new Text(week + " " + newDeckId), new StatsWritable(newDeckId, 2, 1, clan, players, strength));
                context.write(new Text((month + 53) + " " + newDeckId), new StatsWritable(newDeckId, 2, 1, clan, players, strength));
            } else {
                HashSet<String> players1 = new HashSet<>();
                HashSet<String> players2 = new HashSet<>();
                players1.add(value.getPlayerOne().getPlayerId());
                players2.add(value.getPlayerTwo().getPlayerId());

                int victory = (value.getCrownOne() > value.getCrownTwo() ? 1 : 0);

                context.write(new Text(orderedDeck1), new StatsWritable(orderedDeck1, 1, victory, victory * clan, players1, victory * strength));
                context.write(new Text(week + " " + orderedDeck1), new StatsWritable(orderedDeck1, 1, victory, victory * clan, players1, victory * strength));
                context.write(new Text((month + 53) + " " + orderedDeck1), new StatsWritable(orderedDeck1, 1, victory, victory * clan, players1, victory * strength));

                context.write(new Text(orderedDeck2), new StatsWritable(orderedDeck2, 1, 1 - victory, (1 - victory) * clan, players2, (1 - victory) * strength));
                context.write(new Text(week + " " + orderedDeck2), new StatsWritable(orderedDeck2, 1, 1 - victory, (1 - victory) * clan, players2, (1 - victory) * strength));
                context.write(new Text((month + 53) + " " + orderedDeck2), new StatsWritable(orderedDeck2, 1, 1 - victory, (1 - victory) * clan, players2, (1 - victory) * strength));
            }
            
        }

        protected String orderCards(String cards){
            String[] hexValues = cards.split("(?<=\\G..)");
            hexValues = Stream.of(hexValues).sorted().toArray(String[]::new);
            return String.join("", hexValues);
        }
    }

    public static class AnalysisReducer extends Reducer<Text, StatsWritable, Text, StatsWritable>{



        @Override
        protected void setup(Context context){}

        @Override
        public void reduce(Text key, Iterable<StatsWritable> values, Context context) throws IOException, InterruptedException{

            Iterator<StatsWritable> iterator = values.iterator();
            StatsWritable stats =  (StatsWritable) iterator.next().clone();

            while(iterator.hasNext()){
                StatsWritable game = iterator.next();
                stats.games += game.games;
                stats.victories += game.victories;
                for(String player: game.players){
                    stats.players.add(player);
                }
                stats.clan = Math.max(stats.clan, game.clan);
                stats.strength += game.strength;
            }
            context.write(key, stats);
        }
    }

    public static void main(String[] args) throws Exception {
        Configuration conf = new Configuration();
        Job job = Job.getInstance(conf, "CRGameAnalysis");
        job.setNumReduceTasks(1);
        job.setJarByClass(GameAnalysis.class);
        job.setMapperClass(AnalysisMapper.class);
        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(StatsWritable.class);
        job.setReducerClass(AnalysisReducer.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(StatsWritable.class);
        job.setOutputFormatClass(SequenceFileOutputFormat.class);
        job.setInputFormatClass(SequenceFileInputFormat.class);
        FileInputFormat.addInputPath(job, new Path(args[0]));
        FileOutputFormat.setOutputPath(job, new Path(args[1]));
        System.exit(job.waitForCompletion(true) ? 0 : 1);
    }
}