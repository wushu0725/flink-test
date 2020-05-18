package com.shrek.test.flink.windowing;

import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.common.functions.ReduceFunction;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.util.Collector;

/**
 * TODO
 *
 * @author WuShu
 * @date 2020-05-18 17:06
 * @remark
 */
public class WindowWordCount {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();


        DataStream<String> textStream = env.socketTextStream("localhost", 9000, "\n");

        SingleOutputStreamOperator<Tuple2<String, String>> counts = textStream.flatMap(new FlatMapFunction<String, Tuple2<String, String>>() {
            /**
             * 这里是将字符串按字符串分组，再统计字符串的数量输出
             * @param s            输入类型 string
             * @param collector      输出流   输出类型 Tuple2<String, String>， Tuple2是flink定义的自定义类型，当参数有N个时，就用TopleN,目前最多22个
             * @throws Exception
             */
            @Override
            public void flatMap(String s, Collector<Tuple2<String, String>> collector) throws Exception {
                String[] splits = s.toLowerCase().split(",");
                    if (splits.length > 0) {
                        collector.collect(new Tuple2<>(splits[0], splits[1]));
                    }
                }
        }).keyBy(0).countWindow(3,2). //滑动次数窗口，窗口大小为3，滑动距离为2
                reduce(
                new ReduceFunction<Tuple2<String, String>>() {
                    @Override
                    public Tuple2<String, String> reduce(Tuple2<String, String> stringIntegerTuple2, Tuple2<String, String> t1) throws Exception {
                        return t1.of(stringIntegerTuple2.f0, stringIntegerTuple2.f1 + " " + t1.f1);
                    }
                }
        );

        counts.print();

        env.execute("countWindow");

    }
}
