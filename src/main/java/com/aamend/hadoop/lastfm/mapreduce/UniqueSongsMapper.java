package com.aamend.hadoop.lastfm.mapreduce;

import com.aamend.hadoop.lastfm.io.Song;
import org.apache.commons.lang.StringUtils;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;

/**
 * Created by antoine on 6/12/14.
 */
public class UniqueSongsMapper extends Mapper<LongWritable, Text, Song, IntWritable> {

    private static final IntWritable ONE = new IntWritable(1);

    @Override
    public void map(LongWritable key, Text value, Context context)
            throws IOException, InterruptedException {

        String line = value.toString();

        // Do not process header
        if (line.charAt(0) == '#') {
            return;
        }

        // Parse line and get trackId, artId
        String[] fields = line.split("\t", -1);
        if (fields.length < 6) {
            return;
        }

        String artName = fields[3];
        String traId = fields[4];
        String traName = fields[5];

        if (StringUtils.isEmpty(traId)) {
            return;
        }

        // Output Song tuple + count
        context.write(new Song(traId, traName, artName), ONE);
    }


}
