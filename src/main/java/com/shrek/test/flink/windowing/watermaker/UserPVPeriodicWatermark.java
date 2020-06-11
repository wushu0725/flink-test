package com.shrek.test.flink.windowing.watermaker;

import com.shrek.test.flink.entity.UserPV;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.streaming.api.functions.AssignerWithPeriodicWatermarks;
import org.apache.flink.streaming.api.watermark.Watermark;

import javax.annotation.Nullable;

/**
 * TODO
 *
 * @author WuShu
 * @date 2020-05-22 14:59
 * @remark
 */
public class UserPVPeriodicWatermark implements AssignerWithPeriodicWatermarks<Tuple3<String,Long,Integer>> {


    @Nullable
    @Override
    public Watermark getCurrentWatermark() {
        return null;
    }

    @Override
    public long extractTimestamp(Tuple3<String, Long, Integer> tuple3, long l) {
        return 0;
    }
}
