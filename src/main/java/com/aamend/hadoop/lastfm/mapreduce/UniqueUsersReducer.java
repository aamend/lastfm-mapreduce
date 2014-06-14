package com.aamend.hadoop.lastfm.mapreduce;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;
import java.util.HashSet;
import java.util.Set;

/**
 * Created by antoine on 6/12/14.
 */
public class UniqueUsersReducer extends Reducer<Text, Text, Text, IntWritable> {

    private static final IntWritable SUM = new IntWritable();

    @Override
    public void reduce(Text key, Iterable<Text> values, Context context)
            throws IOException, InterruptedException {

        Set<String> traIds = new HashSet<String>();
        for (Text value : values) {
            String traId = value.toString();
            if(!traIds.contains(traId)){
                traIds.add(traId);
            }
        }

        SUM.set(traIds.size());
        context.write(key, SUM);
    }
}

