package com.aamend.hadoop.lastfm.mapreduce;

import com.aamend.hadoop.lastfm.io.Session;
import org.apache.hadoop.io.*;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;

/**
 * Created by antoine on 6/12/14.
 */
public class TopSessionsETLMapper extends Mapper<IntWritable, Session, NullWritable, MapWritable> {

    @Override
    protected void map(IntWritable key, Session value, Context context)
            throws IOException, InterruptedException {
        MapWritable doc = new MapWritable();
        doc.put(new Text("id"), new IntWritable(value.getSessionId()));
        doc.put(new Text("start"), new LongWritable(value.getStartTime()));
        doc.put(new Text("stop"), new LongWritable(value.getStopTime()));
        doc.put(new Text("tracks"), new ArrayWritable(value.getTraIds()));
        doc.put(new Text("count"), new IntWritable(value.getSize()));
        doc.put(new Text("user"), new Text(value.getUserId()));
        context.write(NullWritable.get(), doc);
    }
}
