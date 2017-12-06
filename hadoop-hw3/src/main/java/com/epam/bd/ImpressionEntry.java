package com.epam.bd;

import org.apache.hadoop.io.Writable;
import org.apache.hadoop.io.WritableUtils;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

public class ImpressionEntry implements Writable {
    long cityId;
    int biddingPrice;
    int count;
    String userAgent;

    public ImpressionEntry() {}

    public ImpressionEntry(long cityId, int biddingPrice, int count, String userAgent){
        this.cityId = cityId;
        this.biddingPrice = biddingPrice;
        this.count = count;
    }

    public long getCityId() {
        return cityId;
    }

    public int getBiddingPrice() {
        return biddingPrice;
    }

    public int getCount() {
        return count;
    }

    public String getUserAgent()
    {
        return userAgent;
    }

    public void set(long cityId, int biddingPrice, int count, String userAgent) {
        this.cityId = cityId;
        this.biddingPrice = biddingPrice;
        this.count = count;
        this.userAgent = userAgent;
    }

    public void write(DataOutput dataOutput) throws IOException {
        WritableUtils.writeVLong(dataOutput, cityId);
        WritableUtils.writeVInt(dataOutput, biddingPrice);
        WritableUtils.writeVInt(dataOutput, count);
        WritableUtils.writeString(dataOutput, userAgent);
    }

    public void readFields(DataInput dataInput) throws IOException {
        cityId = WritableUtils.readVLong(dataInput);
        biddingPrice = WritableUtils.readVInt(dataInput);
        count = WritableUtils.readVInt(dataInput);
        userAgent = WritableUtils.readString(dataInput);
    }

    public void aggregate(ImpressionEntry val) {
        if (val.getCityId() == this.cityId) {
            this.count += val.getCount();
        }
    }


    @Override
    public boolean equals(Object o) {
        if (o instanceof ImpressionEntry) {
            ImpressionEntry ie = (ImpressionEntry)o;
            return (this.cityId == ie.cityId)
                    && (this.biddingPrice == ie.biddingPrice)
                    && (this.count == ie.count);
        } else return false;
    }
}
