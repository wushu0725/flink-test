package com.shrek.test.flink.windowing;

import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.timestamps.BoundedOutOfOrdernessTimestampExtractor;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.util.Collector;

/**
 * TODO
 * 滑动窗口测试，滑动窗口是设置窗口大小和滑动距离，比如设置大小为15毫秒，滑动大小为5毫秒，
 * 那么，窗口会每5毫秒关闭窗口，统计窗口大小为15毫秒，也可以设置waterMake水位线，延迟统计来处理分布式的消息乱序问题
 * @author WuShu
 * @date 2020-05-18 18:11
 * @remark
 */
public class SlidingWindowCount {
    public static void main(String[] args) {
        // 设置flink的运行环境，这里会根据本地环境还是生成环境得到不同的对象。
        final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        // 设置时间语义为时间时间
        env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime);

        //并行度1方便看效果
        env.setParallelism(1);

        DataStream<String> textStream = env.socketTextStream("localhost", 9000, "\n");

        SingleOutputStreamOperator<Tuple3<String, Long, Integer>> counts = textStream.flatMap(new FlatMapFunction<String, Tuple3<String, Long, Integer>>() {
            /**
             * 这里是将字符串按字符串分组，再统计字符串的数量输出
             * @param s            输入类型 string
             * @param collector      输出流   输出类型 Tuple2<String, String>， Tuple2是flink定义的自定义类型，当参数有N个时，就用TopleN,目前最多22个
             * @throws Exception
             */
            @Override
            public void flatMap(String s, Collector<Tuple3<String, Long, Integer>> collector) throws Exception {
                String[] splits = s.toLowerCase().split(",");
                if (splits.length > 0) {
                    collector.collect(new Tuple3<>(splits[0],Long.valueOf(splits[1]),new Integer(splits[2])));
                }
            }
        });


//        counts.keyBy(0).assignTimestampsAndWatermarks(new BoundedOutOfOrdernessTimestampExtractor<Tuple3<String, Long, Integer>>() {
//            @Override
//            public long extractTimestamp(Tuple3<String, Long, Integer> stringLongIntegerTuple3) {
//                return stringLongIntegerTuple3.f1;
//            }
//        }).timeWindowAll(Time.milliseconds(2500),Time.milliseconds(500));
    }
}
