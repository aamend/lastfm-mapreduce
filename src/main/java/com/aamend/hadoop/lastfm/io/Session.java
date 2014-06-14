package com.aamend.hadoop.lastfm.io;

import org.apache.hadoop.io.Writable;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.Arrays;
import java.util.List;

/**
 * Created by antoine on 6/12/14.
 */
public class Session implements Writable {

    private String userId;
    private int sessionId;
    private long startTime;
    private long stopTime;
    private int size;
    private String[] traIds;

    public Session() {

    }

    public Session(String userId, int sessionId, long startTime, long stopTime, List<String> tracks) {
        this.userId = userId;
        this.sessionId = sessionId;
        this.startTime = startTime;
        this.stopTime = stopTime;
        this.size = tracks.size();
        this.traIds = tracks.toArray(new String[0]);
    }

    public String getUserId() {
        return userId;
    }

    public void setUserId(String userId) {
        this.userId = userId;
    }

    public String[] getTraIds() {
        return traIds;
    }

    public void setTraIds(String[] traIds) {
        this.traIds = traIds;
    }

    public int getSessionId() {
        return sessionId;
    }

    public void setSessionId(int sessionId) {
        this.sessionId = sessionId;
    }

    public long getStartTime() {
        return startTime;
    }

    public void setStartTime(long startTime) {
        this.startTime = startTime;
    }

    public long getStopTime() {
        return stopTime;
    }

    public void setStopTime(long stopTime) {
        this.stopTime = stopTime;
    }

    public int getSize() {
        return size;
    }

    public void setSize(int size) {
        this.size = size;
    }

    @Override
    public void write(DataOutput out) throws IOException {
        out.writeUTF(userId);
        out.writeInt(sessionId);
        out.writeLong(startTime);
        out.writeLong(stopTime);
        out.writeInt(size);
        for (String traId : traIds) {
            out.writeUTF(traId);
        }
    }

    @Override
    public void readFields(DataInput in) throws IOException {
        this.userId = in.readUTF();
        this.sessionId = in.readInt();
        this.startTime = in.readLong();
        this.stopTime = in.readLong();
        this.size = in.readInt();
        traIds = new String[size];
        for (int i = 0; i < size; i++) {
            traIds[i] = in.readUTF();
        }
    }

    @Override
    public String toString() {
        return "Session{" +
                "userId='" + userId + '\'' +
                ", sessionId=" + sessionId +
                ", startTime=" + startTime +
                ", stopTime=" + stopTime +
                ", size=" + size +
                ", traIds=" + Arrays.toString(traIds) +
                '}';
    }
}
