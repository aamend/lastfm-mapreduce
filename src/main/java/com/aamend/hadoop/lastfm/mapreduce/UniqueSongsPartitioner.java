package com.aamend.hadoop.lastfm.mapreduce;

import com.aamend.hadoop.lastfm.io.Song;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.mapreduce.Partitioner;

/**
 * Created by antoine on 6/12/14.
 */
public class UniqueSongsPartitioner extends Partitioner<Song, IntWritable> {

    @Override
    public int getPartition(Song key, IntWritable value, int numReduceTasks) {
        return (key.getTraId().hashCode() & Integer.MAX_VALUE) % numReduceTasks;
    }
}
