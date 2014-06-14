package com.aamend.hadoop.lastfm.mapreduce;

import com.aamend.hadoop.lastfm.io.Song;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;

/**
 * Created by antoine on 6/12/14.
 */
public class TopSongsReducer extends Reducer<IntWritable, Song, IntWritable, Song> {

    private static final String TOP_N_KEY = "top.n";
    private static final IntWritable TOP = new IntWritable();

    private int n;
    private int count;

    @Override
    public void setup(Context context) {
        n = context.getConfiguration().getInt(TOP_N_KEY, 0);
        count = 0;
    }

    @Override
    public void reduce(IntWritable key, Iterable<Song> values, Context context)
            throws IOException, InterruptedException {

        for (Song value : values) {
            count++;
            if (count > n) {
                return;
            }
            TOP.set(count);
            context.write(TOP, value);
        }
    }
}

