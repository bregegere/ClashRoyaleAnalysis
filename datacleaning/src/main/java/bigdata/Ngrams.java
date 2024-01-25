package bigdata;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaDoubleRDD;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.util.StatCounter;
import org.apache.spark.api.java.function.Function2;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.HColumnDescriptor;
import org.apache.hadoop.hbase.HTableDescriptor;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.Admin;
import org.apache.hadoop.hbase.client.Connection;
import org.apache.hadoop.hbase.client.ConnectionFactory;
import org.apache.hadoop.hbase.client.HBaseAdmin;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.Table;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.hbase.io.ImmutableBytesWritable;
import org.apache.hadoop.hbase.mapreduce.TableOutputFormat;
import org.apache.hadoop.hbase.mapreduce.TableInputFormat;
import org.apache.hadoop.mapreduce.Job;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.stream.*;

import org.apache.hadoop.io.Text;

import scala.Tuple2;


public class Ngrams {
    public static final byte[] DECK_FAMILY = Bytes.toBytes("deck");
    public static final String TABLE_NAME = "tbregegere:sparkDECK2";

    public static void createTable(Connection connect) {
			try {
				final Admin admin = connect.getAdmin();
                //create a table with two column families: city and population
				HTableDescriptor tableDescriptor = new HTableDescriptor(TableName.valueOf(TABLE_NAME));
				tableDescriptor.addFamily(new HColumnDescriptor(DECK_FAMILY));
			    if (admin.tableExists(tableDescriptor.getTableName())) {
				    admin.disableTable(tableDescriptor.getTableName());
    				admin.deleteTable(tableDescriptor.getTableName());
	    		}
		    	admin.createTable(tableDescriptor);
				admin.close();
			} catch (Exception e) {
				e.printStackTrace();
				System.exit(-1);
			}
        }

        public static Tuple2<ImmutableBytesWritable, Put> prepareForHbase(Tuple2<String,String> x) {
            //the first element of the tuple, the city name, will be the key for the row
            //that could be a problem for cities with the same name.
            //However, this is just an example, we don't care.
            //We could use zipWithIndex to add an index to each element of the RDD,
            //that index could be used as a key (and would really be unique).
            Put put = new Put(Bytes.toBytes(x._1()));
            //we'll add two columns for this line, city:name (name is the name of the
            //column, city is the family) and population:total
            String tokens[] = x._2().split(" ");
            put.addColumn(DECK_FAMILY, Bytes.toBytes("deck"), Bytes.toBytes(x._1().split(" ")[1]));
            put.addColumn(DECK_FAMILY, Bytes.toBytes("victories"), Bytes.toBytes(tokens[1]));
            put.addColumn(DECK_FAMILY, Bytes.toBytes("games"), Bytes.toBytes(tokens[2]));
            put.addColumn(DECK_FAMILY, Bytes.toBytes("players"), Bytes.toBytes(tokens[3]));
            put.addColumn(DECK_FAMILY, Bytes.toBytes("clan"), Bytes.toBytes(tokens[4]));
            put.addColumn(DECK_FAMILY, Bytes.toBytes("strength"), Bytes.toBytes(tokens[5]));
            return new Tuple2<ImmutableBytesWritable, Put>(new ImmutableBytesWritable(), put);
        }

    public static void main(String[] args) throws Exception {
        Configuration conf_hbase = HBaseConfiguration.create();
		conf_hbase.set("hbase.mapred.outputtable", TABLE_NAME);
		conf_hbase.set("mapreduce.outputformat.class", "org.apache.hadoop.hbase.mapreduce.TableOutputFormat");
		conf_hbase.set("mapreduce.output.fileoutputformat.outputdir", "/tmp");
		Connection connection = ConnectionFactory.createConnection(conf_hbase);

        SparkConf conf = new SparkConf().setAppName("Ngrams");
        JavaSparkContext context = new JavaSparkContext(conf);

        String tmpDirectory = args[0];

        JavaPairRDD<Text, DeckAnalysisWritable> inputfile = context.sequenceFile(tmpDirectory + "/game-analysis/part-r-00000", Text.class, DeckAnalysisWritable.class);
        JavaRDD<Tuple2<String, String>> rdd1 = inputfile.map((x) -> {
            String text = x._1().toString();
            return new Tuple2<String, String>(text, x._2().text());
        });

        JavaRDD<Tuple2<String, String>> combinaisonsRDD = rdd1.flatMap((x) -> {
            List<Tuple2<String, String>> results = new ArrayList<>();
            String[] tokens = x._1().split(" ");
            if(tokens.length == 2){
                int week = Integer.parseInt(tokens[0]);
                if(week < 53){
                    String deck = tokens[1];
                    List<String> cards = new ArrayList<>(Arrays.asList(deck.split("(?<=\\G..)")));

                    for (int size = 1; size <= 7; size++) {
                        Set<String> combinaisons = generateCombinations(cards, size);

                        for (String ntuple : combinaisons) {
                                String complete_ntuple = week + " " + ntuple;
                                results.add(new Tuple2<>(complete_ntuple, x._2()));
                            }
                        }
                    }
                }
            
            return results.iterator();

        });

        JavaPairRDD<String, String> rddByKey = JavaPairRDD.fromJavaRDD(combinaisonsRDD);

        JavaPairRDD<String, String> rddReduced = rddByKey.reduceByKey(new Function2<String, String, String>() {
            @Override
            public String call(String d1, String d2) throws Exception {
                String[] tokens1 = d1.split(" ");
                String[] tokens2 = d2.split(" ");
                String deck = tokens1[0];
                int victories = Integer.parseInt(tokens1[1]) + Integer.parseInt(tokens2[1]);
                int games = Integer.parseInt(tokens1[2]) + Integer.parseInt(tokens2[2]);
                int clan = Math.max(Integer.parseInt(tokens1[4]), Integer.parseInt(tokens2[4]));
                int players = Integer.parseInt(tokens1[3]) + Integer.parseInt(tokens2[3]);
                double strength = (Double) (Double.parseDouble(tokens1[5]) + Double.parseDouble(tokens2[5])) / 2;

                return deck + " " + victories + " " + games + " " + players + " " + clan + " " + strength;
            }
        });

        System.out.println("Nombre de combinaisons : " + rddReduced.count());

        createTable(connection);

        JavaPairRDD<ImmutableBytesWritable, Put> hbaserdd = rddReduced.mapToPair(x -> prepareForHbase(x));

        Job newAPIJob = Job.getInstance(conf_hbase);
        hbaserdd.saveAsNewAPIHadoopDataset(newAPIJob.getConfiguration());
        System.out.println("saved to hbase\n");

    }

    public static Set<String> generateCombinations(List<String> cards, int size) {
            Set<String> combinations = new HashSet<String>();
            generateCombinationsHelper(cards, size, 0, new ArrayList<>(), combinations);
            return combinations;
        }
    
    public static void generateCombinationsHelper(List<String> cards, int size, int index, List<String> currentCombination, Set<String> combinations) {
            if (size == 0) {
                String[] ordered = Stream.of(currentCombination.toArray()).sorted().toArray(String[]::new);
                combinations.add(String.join("", ordered));
                return;
            }
    
            for (int i = index; i < cards.size(); i++) {
                currentCombination.add(cards.get(i));
                generateCombinationsHelper(cards, size - 1, i + 1, currentCombination, combinations);
                currentCombination.remove(cards.get(i));
            }
    }
        
}
