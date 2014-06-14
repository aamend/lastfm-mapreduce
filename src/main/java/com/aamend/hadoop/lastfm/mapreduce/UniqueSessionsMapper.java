package com.aamend.hadoop.lastfm.mapreduce;

import com.aamend.hadoop.lastfm.io.SessionSong;
import org.apache.commons.lang.StringUtils;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;
import java.text.ParseException;
import java.text.SimpleDateFormat;

/**
 * Created by antoine on 6/12/14.
 */
public class UniqueSessionsMapper extends Mapper<LongWritable, Text, Text, SessionSong> {

    private static final SimpleDateFormat SDF = new SimpleDateFormat("yyyy-MM-dd'T'HH:mm:ss");
    private static final Text USER = new Text();

    @Override
    protected void map(LongWritable key, Text value, Context context)
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

        String userId = fields[0];
        String traId = fields[4];
        String sessionTime = fields[1];

        if (StringUtils.isEmpty(userId) ||
                StringUtils.isEmpty(traId) ||
                StringUtils.isEmpty(sessionTime)) {
            return;
        }

        long timestamp;
        try {
            timestamp = SDF.parse(sessionTime).getTime();
        } catch (ParseException e) {
            return;
        }

        USER.set(userId);
        context.write(USER, new SessionSong(traId, timestamp));
    }

}
