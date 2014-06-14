package com.aamend.hadoop.lastfm.mapreduce;

import com.aamend.hadoop.lastfm.io.Session;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;

/**
 * Created by antoine on 6/12/14.
 */
public class TopSessionsMapper extends Mapper<Session, NullWritable, IntWritable, Session> {

    private static final IntWritable SUM = new IntWritable();

    @Override
    protected void map(Session key, NullWritable value, Context context)
            throws IOException, InterruptedException {

        SUM.set(key.getSize());
        context.write(SUM, key);

    }

}
