package com.qianfeng.analystic.model.dim.base;

import com.qianfeng.common.DateEnum;
import com.qianfeng.util.TimeUtil;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.Calendar;
import java.util.Date;
import java.util.Objects;

/**
 * 定义时间维度
 */
public class DateDimension extends BaseDimension {
    private int id;
    private int year;
    private int season;
    private int month;
    private int week;
    private int day;
    private Date calender = new Date();
    private String type;//什么类型的指标 如天指标 月指标 季度指标

    public DateDimension(int id, int year, int season, int month, int week, int day, Date calender, String type) {
        this.id = id;
        this.year = year;
        this.season = season;
        this.month = month;
        this.week = week;
        this.day = day;
        this.calender = calender;
        this.type = type;
    }

    public DateDimension(int i, int i1, int i2, int i3, String s, String year) {
        this.year = this.year;
        this.season = season;
        this.month = month;
        this.week = week;
        this.day = day;
        this.calender = calender;
    }

    public DateDimension(int id, int year, int season, int month, int week, int day, Date calender) {
        this.id = id;
        this.year = year;
        this.season = season;
        this.month = month;
        this.week = week;
        this.day = day;
        this.calender = calender;
    }

    public DateDimension() {
        this.year = year;
        this.season = season;
        this.month = month;
        this.week = week;
        this.day = day;
        this.calender = calender;
        this.type = type;
    }



    public static DateDimension buildDate(long time, DateEnum type){
        int year = TimeUtil.getDateInfo(time,DateEnum.YEAR);
        Calendar calendar = Calendar.getInstance();
        calendar.clear();//首先要清除一下calendar类型
        //判断type的类型
        if (DateEnum.YEAR.equals(type)){
            //当年的一月一号
            calendar.set(year,1,1);
            return new DateDimension();
        }
        int season = TimeUtil.getDateInfo(time,DateEnum.SEASON);

        if (DateEnum.SEASON.equals(type)){
            //当月第一个季度的1号
            int month=season*3-2;
            calendar.set(year,month-1,1);
            return new DateDimension();
        }
        int month = TimeUtil.getDateInfo(time,DateEnum.MONTH);
        if (DateEnum.MONTH.equals(type)){
            //当月的1月1号
            calendar.set(year,month-1,1);
            return new DateDimension();
        }
        int week = TimeUtil.getDateInfo(time,DateEnum.WEEK);
        if (DateEnum.WEEK.equals(type)){
            //当周的第一天 零时零分零点

            long fristDayOfWeek = TimeUtil.getFirstDayOfWeek(time);
            year = TimeUtil.getDateInfo(fristDayOfWeek,DateEnum.YEAR);
            season = TimeUtil.getDateInfo(fristDayOfWeek,DateEnum.SEASON);
            month = TimeUtil.getDateInfo(fristDayOfWeek,DateEnum.MONTH);
            week = TimeUtil.getDateInfo(fristDayOfWeek,DateEnum.WEEK);
            int day = TimeUtil.getDateInfo(fristDayOfWeek,DateEnum.DAY);
            calendar.set(year,month-1,1);
            return new DateDimension();

        }

        int day = TimeUtil.getDateInfo(time,DateEnum.DAY);
        if (DateEnum.DAY.equals(type)){
            //当月的1号的这一天

            calendar.set(year,month-1,1);
            return new DateDimension();
        }

        throw  new RuntimeException("目前type不支持该类型");
//        return null;
    }

    /**
     * 重写比较器
     * @param o
     * @return
     */

    @Override
    public int compareTo(BaseDimension o) {
        return 0;
    }

    @Override
    public void write(DataOutput dataOutput) throws IOException {
/**
 * 序列化数据
 */
        dataOutput.writeInt(this.id);
        dataOutput.writeInt(this.year);
        dataOutput.writeInt(this.season);
        dataOutput.writeInt(this.month);
        dataOutput.writeInt(this.week);
        dataOutput.writeInt(this.day);
        dataOutput.writeLong(this.calender.getTime());//date类型写成long
        dataOutput.writeUTF(this.type);

    }

    @Override
    public void readFields(DataInput dataInput) throws IOException {

        /**
         * 反序列化
         */

        this.id = dataInput.readInt();
        this.year = dataInput.readInt();
        this.season = dataInput.readInt();
        this.month = dataInput.readInt();
        this.week = dataInput.readInt();
        this.day = dataInput.readInt();
        this.calender.setTime(dataInput.readLong());
        this.type = dataInput.readUTF();
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        DateDimension that = (DateDimension) o;
        return id == that.id &&
                year == that.year &&
                season == that.season &&
                month == that.month &&
                week == that.week &&
                day == that.day &&
                Objects.equals(calender, that.calender) &&
                Objects.equals(type, that.type);
    }

    @Override
    public int hashCode() {

        return Objects.hash(id, year, season, month, week, day, calender, type);
    }

    public int getId() {
        return id;
    }

    public void setId(int id) {
        this.id = id;
    }

    public int getYear() {
        return year;
    }

    public void setYear(int year) {
        this.year = year;
    }

    public int getSeason() {
        return season;
    }

    public void setSeason(int season) {
        this.season = season;
    }

    public int getMonth() {
        return month;
    }

    public void setMonth(int month) {
        this.month = month;
    }

    public int getWeek() {
        return week;
    }

    public void setWeek(int week) {
        this.week = week;
    }

    public int getDay() {
        return day;
    }

    public void setDay(int day) {
        this.day = day;
    }

    public Date getCalender() {
        return calender;
    }

    public void setCalender(Date calender) {
        this.calender = calender;
    }

    public String getType() {
        return type;
    }

    public void setType(String type) {
        this.type = type;
    }
}
