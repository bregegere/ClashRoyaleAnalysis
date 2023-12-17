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
                int compared = Integer.compare(deck1.getPlayers(), deck2.getPlayers());
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


    public static class TopKMapper extends Mapper<Text, DeckAnalysisWritable, Text, DeckAnalysisWritable>{

        private int k;

        //private TreeSet<DeckAnalysisWritable> decks;
        private List<TreeSet<DeckAnalysisWritable>> decks;
        private List<TreeSet<DeckAnalysisWritable>> weekDecksRatio = new ArrayList<>();
        private List<TreeSet<DeckAnalysisWritable>> weekDecksVictories = new ArrayList<>();
        private List<TreeSet<DeckAnalysisWritable>> weekDecksGames = new ArrayList<>();
        private List<TreeSet<DeckAnalysisWritable>> weekDecksPlayers = new ArrayList<>();
        private List<TreeSet<DeckAnalysisWritable>> weekDecksclanMax = new ArrayList<>();
        private List<TreeSet<DeckAnalysisWritable>> weekDecksStrength = new ArrayList<>();

        

        @Override
        protected void setup(Context context){
            Configuration conf = context.getConfiguration();
            k = conf.getInt("k", 100);
            decks = new ArrayList<>();
            /*decks.add(new TreeSet<>(DeckTopK.ratio));
            decks.add(new TreeSet<>(DeckTopK.victories));
            decks.add(new TreeSet<>(DeckTopK.games));
            decks.add(new TreeSet<>(DeckTopK.players));
            decks.add(new TreeSet<>(DeckTopK.clanMax));
            decks.add(new TreeSet<>(DeckTopK.strength));*/
            addAllTypesOfTree(decks);
            for(int i = 0; i < 65; i++){
                weekDecksRatio.add(new TreeSet<>(DeckTopK.ratio));
                weekDecksVictories.add(new TreeSet<>(DeckTopK.victories));
                weekDecksGames.add(new TreeSet<>(DeckTopK.games));
                weekDecksPlayers.add(new TreeSet<>(DeckTopK.players));
                weekDecksclanMax.add(new TreeSet<>(DeckTopK.clanMax));
                weekDecksStrength.add(new TreeSet<>(DeckTopK.strength));
            }
        }

        protected void addAllTypesOfTree(List<TreeSet<DeckAnalysisWritable>> list){
            list.add(new TreeSet<>(DeckTopK.ratio));
            list.add(new TreeSet<>(DeckTopK.victories));
            list.add(new TreeSet<>(DeckTopK.games));
            list.add(new TreeSet<>(DeckTopK.players));
            list.add(new TreeSet<>(DeckTopK.clanMax));
            list.add(new TreeSet<>(DeckTopK.strength));
        }

        @Override
        public void map(Text key, DeckAnalysisWritable value, Context context) throws IOException, InterruptedException{
            try{
                int topKey = -1;
                String[] tokens = key.toString().split(" ");
                if(tokens.length == 2) topKey = Integer.parseInt(tokens[0]);
                DeckAnalysisWritable deckClone = (DeckAnalysisWritable) value.clone();
                addToAllTrees(deckClone, topKey);
            } catch (CloneNotSupportedException e){
                context.getCounter("debug", "clone").increment(1);
            }
        }

        protected void addToAllTrees(DeckAnalysisWritable deckClone, int topKey){
            if(topKey == -1){
                for(int i = 0; i < 6; i++){
                    decks.get(i).add(deckClone);
                    if(decks.get(i).size() > k) decks.get(i).remove(decks.get(i).first());
                }
            } else {
                weekDecksRatio.get(topKey).add(deckClone);
                if(weekDecksRatio.get(topKey).size() > k) weekDecksRatio.get(topKey).remove(weekDecksRatio.get(topKey).first());
                weekDecksVictories.get(topKey).add(deckClone);
                if(weekDecksVictories.get(topKey).size() > k) weekDecksVictories.get(topKey).remove(weekDecksVictories.get(topKey).first());
                weekDecksGames.get(topKey).add(deckClone);
                if(weekDecksGames.get(topKey).size() > k) weekDecksGames.get(topKey).remove(weekDecksGames.get(topKey).first());
                weekDecksPlayers.get(topKey).add(deckClone);
                if(weekDecksPlayers.get(topKey).size() > k) weekDecksPlayers.get(topKey).remove(weekDecksPlayers.get(topKey).first());
                weekDecksclanMax.get(topKey).add(deckClone);
                if(weekDecksclanMax.get(topKey).size() > k) weekDecksclanMax.get(topKey).remove(weekDecksclanMax.get(topKey).first());
                weekDecksStrength.get(topKey).add(deckClone);
                if(weekDecksStrength.get(topKey).size() > k) weekDecksStrength.get(topKey).remove(weekDecksStrength.get(topKey).first());
            }
        }


        @Override
        protected void cleanup(Context context) throws IOException, InterruptedException{
            for(int i = 0; i < 6; i++){
                for(DeckAnalysisWritable deck: decks.get(i)){
                    Text key = new Text(0 + "-" + i);
                    context.write(key, deck);
                }
            }
            for(int i = 0; i < 65; i++){
                
                for(DeckAnalysisWritable deck: weekDecksRatio.get(i)){
                    Text key = new Text(i+1 + "-" + 0);
                    context.write(key, deck);
                }
                for(DeckAnalysisWritable deck: weekDecksVictories.get(i)){
                    Text key = new Text(i+1 + "-" + 1);
                    context.write(key, deck);
                }
                for(DeckAnalysisWritable deck: weekDecksGames.get(i)){
                    Text key = new Text(i+1 + "-" + 2);
                    context.write(key, deck);
                }
                for(DeckAnalysisWritable deck: weekDecksPlayers.get(i)){
                    Text key = new Text(i+1 + "-" + 3);
                    context.write(key, deck);
                }
                for(DeckAnalysisWritable deck: weekDecksclanMax.get(i)){
                    Text key = new Text(i+1 + "-" + 4);
                    context.write(key, deck);
                }
                for(DeckAnalysisWritable deck: weekDecksStrength.get(i)){
                    Text key = new Text(i+1 + "-" + 5);
                    context.write(key, deck);
                }
            }
        }
    }

    public static class TopKReducer extends TableReducer<Text, DeckAnalysisWritable, Text>{

        private int k;

        @Override
        protected void setup(Context context){
            Configuration conf = context.getConfiguration();
            k = conf.getInt("k", 100);
        }

        @Override
        public void reduce(Text key, Iterable<DeckAnalysisWritable> values, Context context) throws IOException, InterruptedException{
            String[] tokens = key.toString().split("-");
            int week = Integer.parseInt(tokens[0]);
            int criteria = Integer.parseInt(tokens[1]);
            TreeSet<DeckAnalysisWritable> decks;
            switch(criteria){
                case 0:
                    decks = new TreeSet<>(DeckTopK.ratio);
                    break;
                case 1:
                    decks = new TreeSet<>(DeckTopK.victories);
                    break;
                case 2:
                    decks = new TreeSet<>(DeckTopK.games);
                    break;
                case 3:
                    decks = new TreeSet<>(DeckTopK.players);
                    break;
                case 4:
                    decks = new TreeSet<>(DeckTopK.clanMax);
                    break;
                case 5:
                    decks = new TreeSet<>(DeckTopK.strength);
                    break;
                default:
                    decks = new TreeSet<>(DeckTopK.ratio);
                    break;
            }
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
                String rowKey;
                if(week == 0){
                    rowKey = "global-" + criteria +  " #" + i;
                    //global-1 #31
                } else if(week < 54){
                    rowKey = "week" + week + "-" + criteria + " #" + i;
                    //week40-1 #31
                } else {
                    rowKey = "month" + (week - 53) + "-" + criteria + " #" + i;
                }
                Put put = new Put(Bytes.toBytes(rowKey));
                put.addColumn(Bytes.toBytes("deck"), Bytes.toBytes("id"), Bytes.toBytes(deck.getDeck()));
                put.addColumn(Bytes.toBytes("deck"), Bytes.toBytes("victories"), Bytes.toBytes(Integer.toString(deck.getVictories())));
                put.addColumn(Bytes.toBytes("deck"), Bytes.toBytes("games"), Bytes.toBytes(Integer.toString(deck.getGames())));
                put.addColumn(Bytes.toBytes("deck"), Bytes.toBytes("players"), Bytes.toBytes(Integer.toString(deck.getPlayers())));
                put.addColumn(Bytes.toBytes("deck"), Bytes.toBytes("clanMax"), Bytes.toBytes(Integer.toString(deck.getClan())));
                put.addColumn(Bytes.toBytes("deck"), Bytes.toBytes("strength"), Bytes.toBytes(Double.toString(deck.getDeltaStrength())));
                
                context.write(key, put);
                i++;
            }
        }


    }

    public static void main(String[] args) throws Exception {

        Configuration conf = new HBaseConfiguration();
        int k = 100;
        TABLE_NAME = args[1];
        conf.setInt("k", k);
        Job job = Job.getInstance(conf, "DeckTopK");
        job.setNumReduceTasks(1);
        job.setJarByClass(DeckTopK.class);
        Connection connection = ConnectionFactory.createConnection(conf);
		createTable(connection);
        job.setMapperClass(TopKMapper.class);
        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(DeckAnalysisWritable.class);
        job.setInputFormatClass(SequenceFileInputFormat.class);
        FileInputFormat.addInputPath(job, new Path(args[0]));

        TableMapReduceUtil.initTableReducerJob(TABLE_NAME, TopKReducer.class, job);

        System.exit(job.waitForCompletion(true) ? 0 : 1);
    }
}