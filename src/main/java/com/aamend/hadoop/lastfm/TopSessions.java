package com.aamend.hadoop.lastfm;

import com.aamend.hadoop.lastfm.io.Session;
import com.aamend.hadoop.lastfm.io.SessionSong;
import com.aamend.hadoop.lastfm.mapreduce.*;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.SequenceFileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.SequenceFileOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

/**
 * Created by antoine on 6/12/14.
 * Say we define a user’s “session” of Last.fm usage to be comprised of one or more
 * songs played by that user, where each song is started within 20 minutes of the
 * previous song’s start time. Create a list of the top 100 longest sessions, with the
 * following information about each session: userid, timestamp of first and last
 * songs in the session, and the list of songs played in the session (in order of play).
 */

public class TopSessions extends Configured implements Tool {

    public static String usage() {
        return "usage : <dataDir> <outputDir> -D top.n=<topN>";
    }

    public static void main(String[] args) throws Exception {
        int res = ToolRunner.run(new Configuration(), new TopSessions(), args);
        System.exit(res);
    }

    @Override
    public int run(String[] args) throws Exception {

        if (args == null || args.length < 2) {
            System.out.println(usage());
            System.exit(1);
        }

        String jobName = "last.fm.sessions";
        Path inputPath = new Path(args[0]);
        Path outputPath = new Path(args[1]);
        Path sessionPath = new Path(outputPath, "sessions");
        Path topNPath = new Path(outputPath, "topN");

        Configuration conf = this.getConf();

        Job job1 = new Job(conf, jobName + "_1");
        job1.setJarByClass(TopSessions.class);

        job1.setInputFormatClass(TextInputFormat.class);
        job1.setOutputFormatClass(SequenceFileOutputFormat.class);
        FileInputFormat.addInputPath(job1, inputPath);
        SequenceFileOutputFormat.setOutputPath(job1, sessionPath);

        job1.setMapperClass(UniqueSessionsMapper.class);
        job1.setReducerClass(UniqueSessionsReducer.class);

        // Reducers (default 1) set from -D mapred.reducer.tasks options

        job1.setMapOutputKeyClass(Text.class);
        job1.setMapOutputValueClass(SessionSong.class);
        job1.setOutputKeyClass(Session.class);
        job1.setOutputValueClass(NullWritable.class);

        // Execute 1st job
        if (!job1.waitForCompletion(true)) {
            return 1;
        }

        Job job2 = new Job(conf, jobName + "_2");
        job2.setJarByClass(TopSessions.class);

        job2.setInputFormatClass(SequenceFileInputFormat.class);
        job2.setOutputFormatClass(SequenceFileOutputFormat.class);
        SequenceFileInputFormat.addInputPath(job2, sessionPath);
        SequenceFileOutputFormat.setOutputPath(job2, topNPath);

        job2.setMapperClass(TopSessionsMapper.class);
        job2.setCombinerClass(TopSessionsCombiner.class);
        job2.setReducerClass(TopSessionsReducer.class);

        job2.setNumReduceTasks(1);
        job2.setSortComparatorClass(TopNComparator.class);

        job2.setMapOutputKeyClass(IntWritable.class);
        job2.setMapOutputValueClass(Session.class);
        job2.setOutputKeyClass(IntWritable.class);
        job2.setOutputValueClass(Session.class);

        // Execute 2nd job
        return job2.waitForCompletion(true) ? 0 : 1;

    }

}
