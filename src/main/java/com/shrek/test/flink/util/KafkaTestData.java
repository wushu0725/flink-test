package com.shrek.test.flink.util;

import com.alibaba.fastjson.JSONObject;
import com.shrek.test.flink.client.KafkaClient;
import com.shrek.test.flink.entity.UserPV;
import com.shrek.test.flink.windowing.watermaker.PeriodicWatermark;
import com.shrek.test.flink.windowing.watermaker.UserPVPeriodicWatermark;
import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer011;
import org.apache.flink.util.Collector;
import org.apache.kafka.clients.producer.Producer;
import org.omg.CORBA.TIMEOUT;

import java.util.Date;
import java.util.Properties;
import java.util.Random;

/**
 * TODO
 * 向kafka里生产测试数据
 * @author WuShu
 * @date 2020-05-21 16:30
 * @remark
 */
public class KafkaTestData {

    //监听用户行为

    public static void main(String[] args) throws Exception {

        final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime);

        DataStreamSource<String> dataStreamSource = env.addSource(KafkaClient.getFlinkKafkaConsumer011("login_user")).setParallelism(1);

        SingleOutputStreamOperator<Tuple3<String, Long, Integer>> counts =
                dataStreamSource.flatMap(new FlatMapFunction<String, Tuple3<String, Long, Integer>>() {
            /**
             * 这里是将字符串按字符串分组，再统计字符串的数量输出
             * @param s            输入类型 string
             * @param collector      输出流   输出类型 Tuple2<String, String>， Tuple2是flink定义的自定义类型，当参数有N个时，就用TopleN,目前最多22个
             * @throws Exception
             */
            @Override
            public void flatMap(String s, Collector<Tuple3<String, Long, Integer>> collector) throws Exception {
                UserPV userPV = JSONObject.parseObject(s, UserPV.class);
//                System.out.println(new Date(userPV.getLogTime()));
                collector.collect(new Tuple3<>(userPV.getApp(),Long.valueOf(userPV.getLogTime()),1));

            }}).assignTimestampsAndWatermarks(new PeriodicWatermark());

        SingleOutputStreamOperator<Tuple3<String, Long, Integer>> sum = counts.keyBy(0).timeWindow(Time.seconds(60)).sum(2);

        sum.print();
//        counts.print();
//        map.print(); //把从 kafka 读取到的数据打印在控制台

        env.execute("Flink add data source");
    }
}
