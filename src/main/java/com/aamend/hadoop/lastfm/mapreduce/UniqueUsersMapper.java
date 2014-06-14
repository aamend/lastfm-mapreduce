package com.aamend.hadoop.lastfm.mapreduce;

import org.apache.commons.lang.StringUtils;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;

/**
 * Created by antoine on 6/12/14.
 */
public class UniqueUsersMapper extends Mapper<LongWritable, Text, Text, Text> {

    private static final Text KEY = new Text();
    private static final Text VAL = new Text();

    @Override
    public void map(LongWritable key, Text value, Context context)
            throws IOException, InterruptedException {

        String line = value.toString();

        // Do not process header
        if (line.charAt(0) == '#') {
            return;
        }

        // Parse line and get userId
        String[] fields = line.split("\t", -1);
        String userId = fields[0];
        String traId = fields[4];
        if (StringUtils.isEmpty(userId) || StringUtils.isEmpty(traId)) {
            return;
        }

        // Output userId + traId
        KEY.set(userId);
        VAL.set(traId);
        context.write(KEY, VAL);
    }
}
