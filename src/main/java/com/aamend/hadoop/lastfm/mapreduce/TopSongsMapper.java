package com.aamend.hadoop.lastfm.mapreduce;

import com.aamend.hadoop.lastfm.io.Song;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;

/**
 * Created by antoine on 6/12/14.
 */
public class TopSongsMapper extends Mapper<Song, IntWritable, IntWritable, Song> {

    @Override
    public void map(Song key, IntWritable value, Context context)
            throws IOException, InterruptedException {
        context.write(value, key);
    }
}
