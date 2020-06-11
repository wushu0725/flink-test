package com.shrek.test.flink.windowing.watermaker;

import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.streaming.api.functions.AssignerWithPeriodicWatermarks;
import org.apache.flink.streaming.api.watermark.Watermark;

import javax.annotation.Nullable;

/**
 * TODO
 * 周期性的水位线
 * @author WuShu
 * @date 2020-05-20 16:19
 * @remark
 */
public class PeriodicWatermark implements AssignerWithPeriodicWatermarks<Tuple3<String, Long, Integer>> {

    private long currentTimestamp = Long.MIN_VALUE;

    @Nullable
    @Override
    public Watermark getCurrentWatermark() {
        long maxTimeLag = 0;
//        System.out.println("水位线"+(currentTimestamp == Long.MIN_VALUE ? Long.MIN_VALUE : currentTimestamp - maxTimeLag));
        return new Watermark(currentTimestamp == Long.MIN_VALUE ? Long.MIN_VALUE : currentTimestamp - maxTimeLag);
    }

    @Override
    public long extractTimestamp(Tuple3<String, Long, Integer> stringLongIntegerTuple3, long l) {
        long timestamp = stringLongIntegerTuple3.f1;
        currentTimestamp = Math.max(timestamp, currentTimestamp);
        return stringLongIntegerTuple3.f1;
    }
}
