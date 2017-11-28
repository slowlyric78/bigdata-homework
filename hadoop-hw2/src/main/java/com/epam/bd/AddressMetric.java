package com.epam.bd;

import org.apache.hadoop.io.Writable;
import org.apache.hadoop.io.WritableUtils;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

public class AddressMetric implements Writable {

    long totalBytes;
    int count;

    public AddressMetric() { super(); }

    public AddressMetric(long totalBytes, int count) {
        this.totalBytes = totalBytes;
        this.count = count;
    }

    public long getTotalBytes() {return totalBytes;}
    public int getCount() {return count;}
    public void set(long totalBytes, int count) {this.totalBytes = totalBytes; this.count = count;}

    public void readFields(DataInput dataInput) throws IOException {
        totalBytes = WritableUtils.readVLong(dataInput);
        count = WritableUtils.readVInt(dataInput);
    }

    public void write(DataOutput dataOutput) throws IOException {
        WritableUtils.writeVLong(dataOutput, totalBytes);
        WritableUtils.writeVInt(dataOutput, count);

    }

    public void set(AddressMetric m) {
        this.totalBytes = m.getTotalBytes();
        this.count = m.getCount();
    }

    public void aggregate(AddressMetric m)
    {
        this.totalBytes += m.getTotalBytes();
        this.count += m.getCount();
    }

    public double average()
    {
        return Utils.calculateAverage(totalBytes, count);
    }

    @Override
    public boolean equals(Object o) {
        if (o instanceof AddressMetric) {
            AddressMetric am = (AddressMetric)o;
            return (this.totalBytes == am.totalBytes) && (this.count == am.count);
        } else return false;
    }
}
