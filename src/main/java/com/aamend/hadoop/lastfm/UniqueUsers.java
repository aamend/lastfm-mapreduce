package com.aamend.hadoop.lastfm;

import com.aamend.hadoop.lastfm.mapreduce.UniqueUsersCombiner;
import com.aamend.hadoop.lastfm.mapreduce.UniqueUsersMapper;
import com.aamend.hadoop.lastfm.mapreduce.UniqueUsersReducer;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.SequenceFileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

/**
 * Created by antoine on 6/12/14.
 * Create a list of unique users, and the number of distinct songs played by each user.
 */
public class UniqueUsers extends Configured implements Tool {

    public static String usage() {
        return "usage : <dataDir> <outputDir>";
    }

    public static void main(String[] args) throws Exception {
        int res = ToolRunner.run(new Configuration(), new UniqueUsers(), args);
        System.exit(res);
    }

    @Override
    public int run(String[] args) throws Exception {

        if (args == null || args.length < 2) {
            System.out.println(usage());
            System.exit(1);
        }

        Path inputPath = new Path(args[0]);
        Path outputPath = new Path(args[1]);

        Configuration conf = this.getConf();
        Job job = new Job(conf, "last.fm.users");
        job.setJarByClass(UniqueUsers.class);

        job.setInputFormatClass(TextInputFormat.class);
        job.setOutputFormatClass(SequenceFileOutputFormat.class);
        FileInputFormat.addInputPath(job, inputPath);
        SequenceFileOutputFormat.setOutputPath(job, outputPath);

        job.setMapperClass(UniqueUsersMapper.class);
        job.setCombinerClass(UniqueUsersCombiner.class);
        job.setReducerClass(UniqueUsersReducer.class);

        // Reducers (default 1) set from -D mapred.reducer.tasks options

        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(Text.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(IntWritable.class);

        return job.waitForCompletion(true) ? 0 : 1;
    }
}
