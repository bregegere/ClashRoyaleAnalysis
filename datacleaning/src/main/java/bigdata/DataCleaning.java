package bigdata;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

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


public class DataCleaning 
{

    public static class DCMapper extends Mapper<LongWritable, Text, Text, GameWritable>{

        private boolean first = true;

        @Override
        public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException{
            JSONObject object = (JSONObject) JSONValue.parse(value.toString());

            String date, cards1, cards2, id1, id2;
            double strength1, strength2;
            int clan1, clan2, crown1, crown2;
            DeckWritable dw1, dw2;
            PlayerWritable p1, p2;
            int round;

            try {
                date = (String) object.get("date");
                cards1 = (String) object.get("cards"); cards2 = (String) object.get("cards2");
                if(cards1.length() > 16 || cards2.length() > 16) return;
                id1 = (String) object.get("player"); id2 = (String) object.get("player2");

            } catch (Exception e){
                context.getCounter("Debug", "date-error").increment(1);
                return;
            }

            try {
                strength1 = ((Number) object.get("deck")).doubleValue(); strength2 = ((Number) object.get("deck2")).doubleValue();
            } catch (Exception e){
                context.getCounter("Debug", "double-error").increment(1);
                if(first){
                    System.out.println(e);
                    //System.out.println(strength1 + " " + strength2);
                    first = false;
                }
                return;
            }

            try {
                dw1 = new DeckWritable(cards1, strength1); dw2 = new DeckWritable(cards2, strength2);
            } catch (Exception e){
                context.getCounter("Debug", "Deck-error").increment(1);
                if(first){
                    System.out.println(e);
                    //System.out.println(strength1 + " " + strength2);
                    first = false;
                }
                return;
            }

            try {
                clan1 = ((Long)object.get("clanTr")).intValue(); clan2 = ((Long)object.get("clanTr2")).intValue();
                /*if(clan1 == null) clan1 = 0;
                if(clan2 == null) clan2 = 0;*/
                p1 = new PlayerWritable(id1, dw1, clan1); p2 = new PlayerWritable(id2, dw2, clan2);

            } catch (Exception e){
                context.getCounter("Debug", "Player-error").increment(1);
                return;
            }

            try {
                crown1 = ((Long)object.get("crown")).intValue(); crown2 = ((Long)object.get("crown2")).intValue();
                round = ((Long)object.get("round")).intValue();
            } catch (Exception e){
                context.getCounter("Debug", "crown-error").increment(1);
                return;
            }
            GameWritable game = new GameWritable(date, p1, p2, crown1, crown2);
            Text dateKey = new Text(date + "-" + round);
            context.write(dateKey, game);
            
        }
    }

    public static class DCReducer extends Reducer<Text, GameWritable, NullWritable, GameWritable>{

        /*@Override
        protected void setup(org.apache.hadoop.mapreduce.Mapper.Context context){}*/

        @Override
        public void reduce(Text key, Iterable<GameWritable> values, Context context) throws IOException, InterruptedException{
            List<String> players = new ArrayList<>();
            for(GameWritable game: values){
                String p1 = game.getPlayerOne().getPlayerId();
                String p2 = game.getPlayerTwo().getPlayerId();
                if(players.size() == 0){
                    players.add(p1); players.add(p2);
                    context.write(NullWritable.get(), game);
                } else {
                    if(!(players.contains(p1) || players.contains(p2))){
                        players.add(p1); players.add(p2);
                        context.write(NullWritable.get(), game);
                    }
                }
            }
        }
    }

    public static void main(String[] args) throws Exception {
        Configuration conf = new Configuration();
        Job job = Job.getInstance(conf, "CRDataCleaning");
        job.setNumReduceTasks(1);
        job.setJarByClass(DataCleaning.class);
        job.setMapperClass(DCMapper.class);
        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(GameWritable.class);
        job.setReducerClass(DCReducer.class);
        job.setOutputKeyClass(NullWritable.class);
        job.setOutputValueClass(GameWritable.class);
        job.setOutputFormatClass(SequenceFileOutputFormat.class);
        job.setInputFormatClass(TextInputFormat.class);
        FileInputFormat.addInputPath(job, new Path(args[0]));
        FileOutputFormat.setOutputPath(job, new Path(args[1]));
        System.exit(job.waitForCompletion(true) ? 0 : 1);
    }
}
