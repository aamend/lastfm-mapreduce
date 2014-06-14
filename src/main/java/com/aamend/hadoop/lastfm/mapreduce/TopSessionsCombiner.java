package com.aamend.hadoop.lastfm.mapreduce;

import com.aamend.hadoop.lastfm.io.Session;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;

/**
 * Created by antoine on 6/12/14.
 */
public class TopSessionsCombiner extends Reducer<IntWritable, Session, IntWritable, Session> {

    private static final String TOP_N_KEY = "top.n";

    private int n;
    private int count;

    @Override
    public void setup(Context context) {
        n = context.getConfiguration().getInt(TOP_N_KEY, 0);
        count = 0;
    }

    @Override
    protected void reduce(IntWritable key, Iterable<Session> values, Context context)
            throws IOException, InterruptedException {

        for (Session value : values) {
            count++;
            if (count > n) {
                return;
            }
            context.write(key, value);
        }
    }
}
