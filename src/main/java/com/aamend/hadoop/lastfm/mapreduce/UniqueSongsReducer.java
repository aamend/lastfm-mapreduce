package com.aamend.hadoop.lastfm.mapreduce;

import com.aamend.hadoop.lastfm.io.Song;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;

/**
 * Created by antoine on 6/12/14.
 */
public class UniqueSongsReducer extends Reducer<Song, IntWritable, Song, IntWritable> {

    private static final IntWritable SUM = new IntWritable();

    @Override
    public void reduce(Song key, Iterable<IntWritable> values, Context context)
            throws IOException, InterruptedException {

        int sum = 0;
        for (IntWritable value : values) {
            sum += value.get();
        }

        SUM.set(sum);
        context.write(key, SUM);
    }
}

