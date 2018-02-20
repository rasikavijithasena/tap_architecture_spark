package com.spark.kafka;

/**
 * Created by cloudera on 11/14/17.
 */
public class DailySummary {
    private String date;
    private String sp_id;
    private String num;

    public String getDate() {
        return date;
    }

    public void setDate(String date) {
        this.date = date;
    }

    public String getSp_id() {
        return sp_id;
    }

    public void setSp_id(String sp_id) {
        this.sp_id = sp_id;
    }

    public String getNum() {
        return num;
    }

    public void setNum(String num) {
        this.num = num;
    }
}
