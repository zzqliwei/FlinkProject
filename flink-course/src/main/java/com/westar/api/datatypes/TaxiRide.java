package com.westar.api.datatypes;


import org.joda.time.DateTime;
import org.joda.time.format.DateTimeFormat;
import org.joda.time.format.DateTimeFormatter;

import java.io.Serializable;
import java.util.Locale;

/**
 * TaxiRide 表示出租车启动或者停止的时候发送的事件
 */
public class TaxiRide implements Serializable {
    private static transient DateTimeFormatter timeFormatter = DateTimeFormat.forPattern("yyyy-MM-dd HH:mm:ss").withLocale(Locale.US).withZoneUTC();

    // 每一次乘车的唯一标识
    private long rideId;
    //每一辆出租车的唯一标识
    private long taxiId;
    //每一位出租车司机的唯一标识
    private long driverId;
    //标识是启动事件还是停止事件，如果为TRUE，则启动，否则停止
    private boolean isStart;
    //每次乘车时出租车的启动时间
    private DateTime startTime;
    //每次乘车时出租车的停止时间
    //如果是启动事件的话，那么结束时间的值就是 "1970-01-01 00:00:00"
    private DateTime endTime;
    // 出租车启动的时候所在位置的经度
    private float startLon;
    // 出租车启动的时候所在位置的纬度
    private float startLat;
    // 出租车停止的时候所在位置的经度
    private float endLon;
    // 出租车停止的时候所在位置的纬度
    private float endLat;
    //出租车乘客的数量
    private short passengerCnt;

    public TaxiRide() {
        this.startTime = new DateTime();
        this.endTime = new DateTime();
    }
    public static TaxiRide fromString(String line) {
        String[] tokens = line.split(",");
        if (tokens.length != 11) {
            throw new RuntimeException("Invalid record: " + line);
        }

        TaxiRide ride = new TaxiRide();

        try {
            ride.rideId = Long.parseLong(tokens[0]);

            switch (tokens[1]) {
                case "START":
                    ride.isStart = true;
                    // 第 3 个字段表示起始时间
                    ride.startTime = DateTime.parse(tokens[2], timeFormatter);
                    // 第 4 个字段表示结束时间
                    ride.endTime = DateTime.parse(tokens[3], timeFormatter);
                    break;
                case "END":
                    ride.isStart = false;
                    // 第 3 个字段表示结束时间
                    ride.endTime = DateTime.parse(tokens[2], timeFormatter);
                    // 第 4 个字段表示起始时间
                    ride.startTime = DateTime.parse(tokens[3], timeFormatter);
                    break;
                default:
                    throw new RuntimeException("Invalid record: " + line);
            }

            ride.startLon = tokens[4].length() > 0 ? Float.parseFloat(tokens[4]) : 0.0f;
            ride.startLat = tokens[5].length() > 0 ? Float.parseFloat(tokens[5]) : 0.0f;
            ride.endLon = tokens[6].length() > 0 ? Float.parseFloat(tokens[6]) : 0.0f;
            ride.endLat = tokens[7].length() > 0 ? Float.parseFloat(tokens[7]) : 0.0f;
            ride.passengerCnt = Short.parseShort(tokens[8]);
            ride.taxiId = Long.parseLong(tokens[9]);
            ride.driverId = Long.parseLong(tokens[10]);

        } catch (NumberFormatException nfe) {
            throw new RuntimeException("Invalid record: " + line, nfe);
        }

        return ride;
    }

    public void copyData(TaxiRide taxiRide) {
        this.rideId = taxiRide.getRideId();
        this.taxiId = taxiRide.getTaxiId();
        this.driverId = taxiRide.getDriverId();
        this.isStart = taxiRide.isStart();
        this.startTime = taxiRide.getStartTime();
        this.endTime = taxiRide.getEndTime();
        this.startLon = taxiRide.getStartLon();
        this.startLat = taxiRide.getStartLat();
        this.endLon = taxiRide.getEndLon();
        this.endLat = taxiRide.getEndLat();
        this.passengerCnt = taxiRide.getPassengerCnt();
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

    public boolean isStart() {
        return isStart;
    }

    public void setStart(boolean start) {
        isStart = start;
    }

    public DateTime getStartTime() {
        return startTime;
    }

    public void setStartTime(DateTime startTime) {
        this.startTime = startTime;
    }

    public DateTime getEndTime() {
        return endTime;
    }

    public void setEndTime(DateTime endTime) {
        this.endTime = endTime;
    }

    public float getStartLon() {
        return startLon;
    }

    public void setStartLon(float startLon) {
        this.startLon = startLon;
    }

    public float getStartLat() {
        return startLat;
    }

    public void setStartLat(float startLat) {
        this.startLat = startLat;
    }

    public float getEndLon() {
        return endLon;
    }

    public void setEndLon(float endLon) {
        this.endLon = endLon;
    }

    public float getEndLat() {
        return endLat;
    }

    public void setEndLat(float endLat) {
        this.endLat = endLat;
    }

    public short getPassengerCnt() {
        return passengerCnt;
    }

    public void setPassengerCnt(short passengerCnt) {
        this.passengerCnt = passengerCnt;
    }

    @Override
    public boolean equals(Object other) {
        return other instanceof TaxiRide && this.rideId == ((TaxiRide)other).rideId;
    }

    @Override
    public int hashCode() {
        return (int)this.rideId;
    }

    @Override
    public String toString() {
        return "TaxiRide{" +
                "rideId=" + rideId +
                ", taxiId=" + taxiId +
                ", driverId=" + driverId +
                ", isStart=" + isStart +
                ", startTime=" + startTime +
                ", endTime=" + endTime +
                ", startLon=" + startLon +
                ", startLat=" + startLat +
                ", endLon=" + endLon +
                ", endLat=" + endLat +
                ", passengerCnt=" + passengerCnt +
                '}';
    }
}


