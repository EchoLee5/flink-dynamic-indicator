package com.hundsun.art.dynamicrules;

import org.apache.flink.streaming.api.windowing.windows.TimeWindow;

/**
 * @author lisen
 * @desc
 * @date Created in 2021/11/3 19:31
 */
public class WindowHelper {

    public static TimeWindow assignWindows(long eventTime, long windowMillis) {
        long start = TimeWindow.getWindowStartWithOffset(eventTime, 0, windowMillis);
        long end = start + windowMillis;

        return new TimeWindow(start, end);
    }

    public static void main(String[] args) {
        System.out.println(assignWindows(1623283990075L, 10*60*1000));
    }
}
