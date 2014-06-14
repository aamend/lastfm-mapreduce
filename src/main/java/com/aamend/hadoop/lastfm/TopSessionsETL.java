package com.aamend.hadoop.lastfm;

import com.aamend.hadoop.lastfm.mapreduce.TopSessionsETLMapper;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.MapWritable;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.SequenceFileInputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;
import org.elasticsearch.hadoop.mr.EsOutputFormat;

/**
 * Created by antoine on 6/12/14.
 */
public class TopSessionsETL extends Configured implements Tool {

    public static String usage() {
        return "usage : <sessionsPath> -D es.nodes=<esNodes> -D es.resource=<esResources>";
    }

    public static void main(String[] args) throws Exception {
        int res = ToolRunner.run(new Configuration(), new TopSessionsETL(), args);
        System.exit(res);
    }

    @Override
    public int run(String[] args) throws Exception {

        if (args == null || args.length < 1) {
            System.out.println(usage());
            System.exit(1);
        }

        String jobName = "last.fm.sessions.es";
        Path inputPath = new Path(args[0]);

        Configuration conf = this.getConf();

        Job job = new Job(conf, jobName);
        job.setJarByClass(TopSessionsETL.class);

        job.setInputFormatClass(SequenceFileInputFormat.class);
        job.setOutputFormatClass(EsOutputFormat.class);
        SequenceFileInputFormat.addInputPath(job, inputPath);

        job.setMapperClass(TopSessionsETLMapper.class);
        job.setNumReduceTasks(0);

        job.setMapOutputValueClass(MapWritable.class);

        return job.waitForCompletion(true) ? 0 : 1;

    }

}
