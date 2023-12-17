package bigdata;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.SequenceFileOutputFormat;
import org.apache.hadoop.mapreduce.lib.input.SequenceFileInputFormat;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.client.Connection;
import org.apache.hadoop.hbase.client.ConnectionFactory;
import org.apache.hadoop.hbase.mapreduce.TableMapReduceUtil;
import bigdata.GameAnalysis.AnalysisMapper;
import bigdata.GameAnalysis.AnalysisReducer;

import bigdata.DataCleaning.DCMapper;
import bigdata.DataCleaning.DCReducer;
import bigdata.DeckTopK.TopKMapper;
import bigdata.DeckTopK.TopKReducer;

public class CRAnalysis {
    public static void main(String[] args) throws Exception{
        String tmpDirectory = args[0];

        Configuration conf = new Configuration();
        Job job1 = Job.getInstance(conf, "CRDataCleaning");
        job1.setNumReduceTasks(1);
        job1.setJarByClass(DataCleaning.class);
        job1.setMapperClass(DCMapper.class);
        job1.setMapOutputKeyClass(Text.class);
        job1.setMapOutputValueClass(GameWritable.class);
        job1.setReducerClass(DCReducer.class);
        job1.setOutputKeyClass(NullWritable.class);
        job1.setOutputValueClass(GameWritable.class);
        job1.setOutputFormatClass(SequenceFileOutputFormat.class);
        job1.setInputFormatClass(TextInputFormat.class);
        FileInputFormat.addInputPath(job1, new Path("/user/auber/data_ple/clashroyale/gdc_battles.nljson"));
        FileOutputFormat.setOutputPath(job1, new Path(tmpDirectory + "/datacleaning"));
        if(!job1.waitForCompletion(true)){
            System.exit(1);
        }

        Job job2 = Job.getInstance(conf, "CRGameAnalysis");
        job2.setNumReduceTasks(1);
        job2.setJarByClass(GameAnalysis.class);
        job2.setMapperClass(AnalysisMapper.class);
        job2.setMapOutputKeyClass(Text.class);
        job2.setMapOutputValueClass(GameWritable.class);
        job2.setReducerClass(AnalysisReducer.class);
        job2.setOutputKeyClass(Text.class);
        job2.setOutputValueClass(DeckAnalysisWritable.class);
        job2.setOutputFormatClass(SequenceFileOutputFormat.class);
        job2.setInputFormatClass(SequenceFileInputFormat.class);
        FileInputFormat.addInputPath(job2, new Path(tmpDirectory + "/datacleaning/part-r-00000"));
        FileOutputFormat.setOutputPath(job2, new Path(tmpDirectory + "/game-analysis"));
        if(!job2.waitForCompletion(true)){
            System.exit(1);
        }

        Configuration confHBase = new HBaseConfiguration();
        int k = 100;
        String TABLE_NAME = args[1];
        confHBase.setInt("k", k);
        Job job3 = Job.getInstance(confHBase, "DeckTopK");
        job3.setNumReduceTasks(1);
        job3.setJarByClass(DeckTopK.class);
        Connection connection = ConnectionFactory.createConnection(confHBase);
		DeckTopK.createTable(connection, TABLE_NAME);
        job3.setMapperClass(TopKMapper.class);
        job3.setMapOutputKeyClass(Text.class);
        job3.setMapOutputValueClass(DeckAnalysisWritable.class);
        job3.setInputFormatClass(SequenceFileInputFormat.class);
        FileInputFormat.addInputPath(job3, new Path(tmpDirectory + "/game-analysis/part-r-00000"));

        TableMapReduceUtil.initTableReducerJob(TABLE_NAME, TopKReducer.class, job3);

        if(!job3.waitForCompletion(true)){
            System.exit(1);
        }
        FileSystem fs = FileSystem.get(conf);
        fs.delete(new Path(tmpDirectory), true);
        System.exit(0);
    }
}
