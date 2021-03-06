package com.aamend.hadoop.lastfm.mapreduce;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;
import java.util.HashSet;
import java.util.Set;

/**
 * Created by antoine on 6/12/14.
 */
public class UniqueUsersCombiner extends Reducer<Text, Text, Text, Text> {

    private static final Text VAL = new Text();

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

        for(String traId : traIds) {
            VAL.set(traId);
            context.write(key, VAL);
        }
    }
}

