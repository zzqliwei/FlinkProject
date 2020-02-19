package com.westar.api.datatypes;

import org.joda.time.DateTime;
import org.joda.time.format.DateTimeFormat;
import org.joda.time.format.DateTimeFormatter;

import java.io.Serializable;
import java.util.Locale;

/**
 * TaxiFare 表示一个出租车收费的事件
 */
public class TaxiFare implements Serializable {

    private static transient DateTimeFormatter timeFormatter = DateTimeFormat.forPattern("yyyy-MM-dd HH:mm:ss").withLocale(Locale.US).withZoneUTC();

    // 每一次 ride 的唯一标识
    private long rideId;
    // 每一辆出租车的唯一标识
    private long taxiId;
    // 每一个出租车司机的唯一标识
    private long driverId;
    // 每次乘车时出租车启动时间
    private DateTime startTime;
    // 支付类型，CSH or CRD
    private String paymentType;
    // 此次乘车的小费
    private float tip;
    // 此次乘车的路费
    private float tolls;
    // 此次乘车的总共的费用
    private float totalFare;
    //数据解析
    public static TaxiFare fromString(String line) {

        String[] tokens = line.split(",");
        if (tokens.length != 8) {
            throw new RuntimeException("Invalid record: " + line);
        }

        TaxiFare ride = new TaxiFare();

        try {
            ride.rideId = Long.parseLong(tokens[0]);
            ride.taxiId = Long.parseLong(tokens[1]);
            ride.driverId = Long.parseLong(tokens[2]);
            ride.startTime = DateTime.parse(tokens[3], timeFormatter);
            ride.paymentType = tokens[4];
            ride.tip = tokens[5].length() > 0 ? Float.parseFloat(tokens[5]) : 0.0f;
            ride.tolls = tokens[6].length() > 0 ? Float.parseFloat(tokens[6]) : 0.0f;
            ride.totalFare = tokens[7].length() > 0 ? Float.parseFloat(tokens[7]) : 0.0f;
        } catch (NumberFormatException nfe) {
            throw new RuntimeException("Invalid record: " + line, nfe);
        }

        return ride;
    }

    public TaxiFare() {
        this.startTime = new DateTime();
    }

    public long getRideId() {
        return rideId;
    }

    public void setRideId(long rideId) {
        this.rideId = rideId;
    }

    public long getTaxiId() {
        return taxiId;
    }

    public void setTaxiId(long taxiId) {
        this.taxiId = taxiId;
    }

    public long getDriverId() {
        return driverId;
    }

    public void setDriverId(long driverId) {
        this.driverId = driverId;
    }

    public DateTime getStartTime() {
        return startTime;
    }

    public void setStartTime(DateTime startTime) {
        this.startTime = startTime;
    }

    public String getPaymentType() {
        return paymentType;
    }

    public void setPaymentType(String paymentType) {
        this.paymentType = paymentType;
    }

    public float getTip() {
        return tip;
    }

    public void setTip(float tip) {
        this.tip = tip;
    }

    public float getTolls() {
        return tolls;
    }

    public void setTolls(float tolls) {
        this.tolls = tolls;
    }

    public float getTotalFare() {
        return totalFare;
    }

    public void setTotalFare(float totalFare) {
        this.totalFare = totalFare;
    }

    @Override
    public boolean equals(Object other) {
        return other instanceof TaxiFare &&
                this.rideId == ((TaxiFare) other).rideId;
    }

    @Override
    public int hashCode() {
        return (int)this.rideId;
    }

    @Override
    public String toString() {
        return "TaxiFare{" +
                "rideId=" + rideId +
                ", taxiId=" + taxiId +
                ", driverId=" + driverId +
                ", startTime=" + startTime +
                ", paymentType='" + paymentType + '\'' +
                ", tip=" + tip +
                ", tolls=" + tolls +
                ", totalFare=" + totalFare +
                '}';
    }
}
