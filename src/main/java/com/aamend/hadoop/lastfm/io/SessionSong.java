package com.aamend.hadoop.lastfm.io;

import org.apache.hadoop.io.WritableComparable;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

/**
 * Created by antoine on 6/12/14.
 */
public class SessionSong implements WritableComparable {

    private String traId;
    private long timestamp;

    public SessionSong() {

    }

    public SessionSong(String traId, long timestamp) {
        this.traId = traId;
        this.timestamp = timestamp;
    }

    public String getTraId() {
        return traId;
    }

    public void setTraId(String traId) {
        this.traId = traId;
    }

    public long getTimestamp() {
        return timestamp;
    }

    public void setTimestamp(long timestamp) {
        this.timestamp = timestamp;
    }

    @Override
    public int compareTo(Object o) {
        SessionSong that = (SessionSong) o;
        if (this.getTimestamp() > that.getTimestamp()) {
            return 1;
        } else if (this.getTimestamp() < that.getTimestamp()) {
            return -1;
        } else {
            return 0;
        }
    }

    @Override
    public void write(DataOutput out) throws IOException {
        out.writeUTF(traId);
        out.writeLong(timestamp);
    }

    @Override
    public void readFields(DataInput in) throws IOException {
        this.traId = in.readUTF();
        this.timestamp = in.readLong();
    }


    @Override
    public String toString() {
        return "SessionSong{" +
                "traId='" + traId + '\'' +
                ", timestamp=" + timestamp +
                '}';
    }
}
