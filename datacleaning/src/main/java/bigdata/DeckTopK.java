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

import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.mapreduce.TableMapReduceUtil;
import org.apache.hadoop.hbase.mapreduce.TableReducer;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.hbase.client.Connection;
import org.apache.hadoop.hbase.client.ConnectionFactory;
import org.apache.hadoop.hbase.client.Admin;
import org.apache.hadoop.hbase.HTableDescriptor;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.HColumnDescriptor;

public class DeckTopK {

    private static String TABLE_NAME = "tbregegere:clashroyale_test"; //IL FAUT CHANGER LE NAMESPACE

    public static void createOrOverwrite(Admin admin, HTableDescriptor table) throws IOException {
		if (admin.tableExists(table.getTableName())) {
			admin.disableTable(table.getTableName());
			admin.deleteTable(table.getTableName());
		}
		admin.createTable(table);
	}
	public static void createTable(Connection connect) {
		try {
			final Admin admin = connect.getAdmin();
			HTableDescriptor tableDescriptor = new HTableDescriptor(TableName.valueOf(TABLE_NAME));
			HColumnDescriptor fam = new HColumnDescriptor(Bytes.toBytes("deck"));
			tableDescriptor.addFamily(fam);
			createOrOverwrite(admin, tableDescriptor);
			admin.close();
		} catch (Exception e) {
			e.printStackTrace();
			System.exit(-1);
		}
	}

    protected static Comparator ratio = new Comparator<DeckAnalysisWritable>() {
            public int compare(DeckAnalysisWritable deck1, DeckAnalysisWritable deck2){
                int compared = Double.compare(((double) deck1.getVictories()) / deck1.getGames(), ((double) deck2.getVictories()) / deck2.getGames());
                if(compared != 0) return compared;
                else return deck1.getDeck().compareTo(deck2.getDeck());
            }
    };

    protected static  Comparator victories = new Comparator<DeckAnalysisWritable>() {
            public int compare(DeckAnalysisWritable deck1, DeckAnalysisWritable deck2){
                int compared = Integer.compare(deck1.getVictories(), deck2.getVictories());
                if(compared != 0) return compared;
                else return deck1.getDeck().compareTo(deck2.getDeck());
            }
    };

    protected static Comparator games = new Comparator<DeckAnalysisWritable>() {
            public int compare(DeckAnalysisWritable deck1, DeckAnalysisWritable deck2){
                int compared = Integer.compare(deck1.getGames(), deck2.getGames());
                if(compared != 0) return compared;
                else return deck1.getDeck().compareTo(deck2.getDeck());
            }
    };

    protected static Comparator players = new Comparator<DeckAnalysisWritable>() {
            public int compare(DeckAnalysisWritable deck1, DeckAnalysisWritable deck2){
                int compared = Integer.compare(deck1.getPlayers().size(), deck2.getPlayers().size());
                if(compared != 0) return compared;
                else return deck1.getDeck().compareTo(deck2.getDeck());
            }
    };

    protected static Comparator clanMax = new Comparator<DeckAnalysisWritable>() {
            public int compare(DeckAnalysisWritable deck1, DeckAnalysisWritable deck2){
                int compared = Integer.compare(deck1.getClan(), deck2.getClan());
                if(compared != 0) return compared;
                else return deck1.getDeck().compareTo(deck2.getDeck());
            }
    };

    protected static Comparator strength = new Comparator<DeckAnalysisWritable>() {
            public int compare(DeckAnalysisWritable deck1, DeckAnalysisWritable deck2){
                int compared = Double.compare((-1 * deck1.getDeltaStrength()), -1 * deck2.getDeltaStrength());
                if(compared != 0) return compared;
                else return deck1.getDeck().compareTo(deck2.getDeck());
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

    public static class TopKReducer extends TableReducer<NullWritable, DeckAnalysisWritable, IntWritable>{

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
            int i = 1;
            for(DeckAnalysisWritable deck: decks.descendingSet()){
                Put put = new Put(Bytes.toBytes(Integer.toString(i)));
                String idBase = "";
                if(i < 10) idBase = "00";
                else if(id < 100) idBase = "0";
                put.addColumn(Bytes.toBytes("deck"), Bytes.toBytes("id"), Bytes.toBytes(idBase + deck.getDeck()));
                put.addColumn(Bytes.toBytes("deck"), Bytes.toBytes("victories"), Bytes.toBytes(Integer.toString(deck.getVictories())));
                put.addColumn(Bytes.toBytes("deck"), Bytes.toBytes("games"), Bytes.toBytes(Integer.toString(deck.getGames())));
                put.addColumn(Bytes.toBytes("deck"), Bytes.toBytes("players"), Bytes.toBytes(Integer.toString(deck.getPlayersLength())));
                put.addColumn(Bytes.toBytes("deck"), Bytes.toBytes("clanMax"), Bytes.toBytes(Integer.toString(deck.getClan())));
                put.addColumn(Bytes.toBytes("deck"), Bytes.toBytes("strength"), Bytes.toBytes(Double.toString(deck.getDeltaStrength())));
                
                context.write(new IntWritable(i), put);
                i++;
            }
        }
    }

    public static void main(String[] args) throws Exception {
        /*Configuration conf = new Configuration();
        int k = 100;
        String comparator;
        try{
            comparator = args[2];
        } catch (Exception e){
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
        job.setOutputKeyClass(IntWritable.class);
        job.setOutputValueClass(DeckAnalysisWritable.class);
        job.setOutputFormatClass(TextOutputFormat.class);
        job.setInputFormatClass(SequenceFileInputFormat.class);
        FileInputFormat.addInputPath(job, new Path(args[0]));
        FileOutputFormat.setOutputPath(job, new Path(args[1]));
        System.exit(job.waitForCompletion(true) ? 0 : 1);*/

        Configuration conf = new HBaseConfiguration();
        int k = 100;
        String comparator;
        TABLE_NAME = args[1];
        try{
            comparator = args[2];
        } catch (Exception e){
            comparator = "ratio";
        }
        conf.setInt("k", k);
        conf.set("comparator", comparator);
        Job job = Job.getInstance(conf, "DeckTopK");
        job.setNumReduceTasks(1);
        job.setJarByClass(DeckTopK.class);
        Connection connection = ConnectionFactory.createConnection(conf);
		createTable(connection);
        job.setMapperClass(TopKMapper.class);
        job.setMapOutputKeyClass(NullWritable.class);
        job.setMapOutputValueClass(DeckAnalysisWritable.class);
        job.setInputFormatClass(SequenceFileInputFormat.class);
        FileInputFormat.addInputPath(job, new Path(args[0]));

        TableMapReduceUtil.initTableReducerJob(TABLE_NAME, TopKReducer.class, job);

        System.exit(job.waitForCompletion(true) ? 0 : 1);
    }
}