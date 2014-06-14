package com.aamend.hadoop.lastfm.io;

import org.apache.hadoop.io.WritableComparable;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

/**
 * Created by antoine on 6/12/14.
 */
public class Song implements WritableComparable {

    private String traId;
    private String traName;
    private String artName;

    public Song() {

    }

    public Song(String traId, String traName, String artName) {
        this.traId = traId;
        this.traName = traName;
        this.artName = artName;
    }

    public String getTraId() {
        return traId;
    }

    public void setTraId(String traId) {
        this.traId = traId;
    }

    public String getTraName() {
        return traName;
    }

    public void setTraName(String traName) {
        this.traName = traName;
    }

    public String getArtName() {
        return artName;
    }

    public void setArtName(String artName) {
        this.artName = artName;
    }

    @Override
    public int compareTo(Object o) {
        Song that = (Song) o;
        return that.getTraId().compareTo(this.getTraId());
    }

    @Override
    public void write(DataOutput out) throws IOException {
        out.writeUTF(traId);
        out.writeUTF(traName);
        out.writeUTF(artName);
    }

    @Override
    public void readFields(DataInput in) throws IOException {
        this.traId = in.readUTF();
        this.traName = in.readUTF();
        this.artName = in.readUTF();
    }

    @Override
    public String toString() {
        return "Song{" +
                "traId='" + traId + '\'' +
                ", traName='" + traName + '\'' +
                ", artName='" + artName + '\'' +
                '}';
    }
}
