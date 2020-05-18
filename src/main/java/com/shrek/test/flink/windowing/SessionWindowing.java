package com.shrek.test.flink.windowing;

import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.source.SourceFunction;
import org.apache.flink.streaming.api.watermark.Watermark;
import org.apache.flink.streaming.api.windowing.assigners.EventTimeSessionWindows;
import org.apache.flink.streaming.api.windowing.time.Time;

import java.util.ArrayList;
import java.util.List;

/**
 * TODO
 * session窗口   设置事件时间因子，当时间会话相差 一段时间 窗口关闭，输出结果
 * @author WuShu
 * @date 2020-05-18 16:16
 * @remark
 */
public class SessionWindowing {


    public static void main(String[] args) throws Exception {
        // 设置flink的运行环境，这里会根据本地环境还是生成环境得到不同的对象。
        final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        // 设置时间语义为时间时间
        env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime);
        //并行度1方便看效果
        env.setParallelism(1);

        final List<Tuple3<String, Long, Integer>> input = new ArrayList<>();
		input.add(new Tuple3<>("a", 1L, 1));
		input.add(new Tuple3<>("b", 1L, 2));
        input.add(new Tuple3<>("a", 3L, 11));
		input.add(new Tuple3<>("b", 3L, 1));

		input.add(new Tuple3<>("b", 5L, 1));
		input.add(new Tuple3<>("c", 6L, 1));



		input.add(new Tuple3<>("a", 10L, 1));

        input.add(new Tuple3<>("c", 10L, 1));

		input.add(new Tuple3<>("c", 11L, 1));
		input.add(new Tuple3<>("c", 15L, 1));


        DataStream<Tuple3<String, Long, Integer>> source = env.addSource(new SourceFunction<Tuple3<String, Long, Integer>>() {
            @Override
            public void run(SourceContext<Tuple3<String, Long, Integer>> sourceContext) throws Exception {
                for (Tuple3<String, Long, Integer> value : input) {

                    //讲input 作为 source,并讲第二个字段设置为eventTime
                    sourceContext.collectWithTimestamp(value,value.f1);


                    //设置 watermark 水位线 延迟一毫秒发出
                    sourceContext.emitWatermark(new Watermark(value.f1-1));
                }
            }

            @Override
            public void cancel() {

            }
        });
        SingleOutputStreamOperator<Tuple3<String, Long, Integer>> sum =
                source.keyBy(0).   //根据f0字段分组
                window(EventTimeSessionWindows.withGap(Time.milliseconds(3L))). //会话时间 3毫秒
                        sum(2);   //第三个字段求和
        sum.print();
        env.execute("sessionWindow: ");
    }


}
