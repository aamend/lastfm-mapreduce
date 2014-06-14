package com.aamend.hadoop.lastfm;

import com.aamend.hadoop.lastfm.io.Song;
import com.aamend.hadoop.lastfm.mapreduce.*;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.SequenceFileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.SequenceFileOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

/**
 * Created by antoine on 6/12/14.
 * Create a list of the top 100 played songs (artist and title) in the dataset, with the number of times each song was played.
 * <p/>
 * <p/>
 * We need two jobs here:
 * - one to count the distinct songs based on traId
 * - one to sort data (topN)
 */
public class TopSongs extends Configured implements Tool {

    public static String usage() {
        return "usage : <dataDir> <outputDir> -D top.n=<topN>";
    }

    public static void main(String[] args) throws Exception {
        int res = ToolRunner.run(new Configuration(), new TopSongs(), args);
        System.exit(res);
    }

    @Override
    public int run(String[] args) throws Exception {

        if (args == null || args.length < 2) {
            System.out.println(usage());
            System.exit(1);
        }

        String jobName = "last.fm.songs";
        Path inputPath = new Path(args[0]);
        Path outputPath = new Path(args[1]);
        Path songCountPath = new Path(outputPath, "count");
        Path topSongPath = new Path(outputPath, "topN");

        Configuration conf = this.getConf();

        Job job1 = new Job(conf, jobName + "_1");
        job1.setJarByClass(TopSongs.class);

        job1.setInputFormatClass(TextInputFormat.class);
        job1.setOutputFormatClass(SequenceFileOutputFormat.class);
        FileInputFormat.addInputPath(job1, inputPath);
        SequenceFileOutputFormat.setOutputPath(job1, songCountPath);

        job1.setMapperClass(UniqueSongsMapper.class);
        job1.setCombinerClass(UniqueSongsReducer.class);
        job1.setReducerClass(UniqueSongsReducer.class);

        // No need only 1 reducer since we supply partitioner
        // Reducers (default 1) set from -D mapred.reducer.tasks options
        job1.setPartitionerClass(UniqueSongsPartitioner.class);

        job1.setMapOutputKeyClass(Song.class);
        job1.setMapOutputValueClass(IntWritable.class);
        job1.setOutputKeyClass(Song.class);
        job1.setOutputValueClass(IntWritable.class);

        // Execute 1st job
        if (!job1.waitForCompletion(true)) {
            return 1;
        }

        Job job2 = new Job(conf, jobName + "_2");
        job2.setJarByClass(UniqueUsers.class);

        job2.setInputFormatClass(SequenceFileInputFormat.class);
        job2.setOutputFormatClass(SequenceFileOutputFormat.class);
        SequenceFileInputFormat.addInputPath(job2, songCountPath);
        SequenceFileOutputFormat.setOutputPath(job2, topSongPath);

        job2.setMapperClass(TopSongsMapper.class);
        job2.setCombinerClass(TopSongsCombiner.class);
        job2.setReducerClass(TopSongsReducer.class);

        job2.setNumReduceTasks(1);
        job2.setSortComparatorClass(TopNComparator.class);

        job2.setMapOutputKeyClass(IntWritable.class);
        job2.setMapOutputValueClass(Song.class);
        job2.setOutputKeyClass(IntWritable.class);
        job2.setOutputValueClass(Song.class);

        // Execute 2nd job
        return job2.waitForCompletion(true) ? 0 : 1;
    }
}
