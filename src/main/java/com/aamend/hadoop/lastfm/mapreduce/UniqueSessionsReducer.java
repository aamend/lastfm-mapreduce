package com.aamend.hadoop.lastfm.mapreduce;

import com.aamend.hadoop.lastfm.io.Session;
import com.aamend.hadoop.lastfm.io.SessionSong;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

/**
 * Created by antoine on 6/12/14.
 */
public class UniqueSessionsReducer extends Reducer<Text, SessionSong, Session, NullWritable> {

    @Override
    protected void reduce(Text key, Iterable<SessionSong> values, Context context)
            throws IOException, InterruptedException {

        List<SessionSong> songs = new ArrayList<SessionSong>();
        for (SessionSong value : values) {
            songs.add(new SessionSong(value.getTraId(), value.getTimestamp()));
        }

        // Our collection might now contain one or several sessions
        // Order all tracks by Timestamp ASC
        Collections.sort(songs);

        int sessionId = 1;
        List<String> tracks = new ArrayList<String>();
        SessionSong firstSong = songs.get(0);

        long firstTimestamp = firstSong.getTimestamp();
        long previousTimestamp = firstSong.getTimestamp();

        // Add our first song to the first session
        tracks.add(firstSong.getTraId());

        Session session;
        long delta = 20 * 60 * 1000;

        // Iterate remaining songs
        for (int i = 1; i < songs.size(); i++) {

            SessionSong song = songs.get(i);
            if (song.getTimestamp() - previousTimestamp > delta) {
                // This is considered as a new session
                // Save previous session ...
                session = new Session(key.toString(), sessionId, firstTimestamp, previousTimestamp, tracks);
                context.write(session, NullWritable.get());
                // ... and start a new one
                sessionId++;
                firstTimestamp = song.getTimestamp();
                tracks = new ArrayList<String>();
            }
            tracks.add(song.getTraId());
            previousTimestamp = song.getTimestamp();
        }

        // Add remainder tracks to a last session
        if (!tracks.isEmpty()) {
            session = new Session(key.toString(), sessionId, firstTimestamp, previousTimestamp, tracks);
            context.write(session, NullWritable.get());
        }
    }
}
