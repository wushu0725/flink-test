package com.shrek.test.flink.wordcount;

import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.util.Collector;

/**
 * TODO
 * 监听端口  字符串的单词出现次数
 * 一些常用关于DataStream算子的博客可以访问 https://blog.csdn.net/chybin500/article/details/87260869  这个博客说的很详细
 * @author WuShu
 * @date 2020-05-18 14:38
 * @remark
 */
public class WordCountMain {

    public static void main(String[] args) throws Exception {

        // 设置flink的运行环境，这里会根据本地环境还是生成环境得到不同的对象。
        final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        //设置并行度1，默认是8
//        env.setParallelism(1);

        //定义加载或创建数据源（source）,监听9000端口的socket消息
        //百度window上安装 netcat ，装完之后命令行 执行 nc -L -p 9000
        DataStream<String> textStream = env.socketTextStream("localhost", 9000, "\n");

        //这里的输出类型是String，所以DataStream泛型是String
        //map算子是 1对1的  输入对输出
        //这里将s封装前面加了 (index++) + ".您输入的是："，一般都是转化为对象
//        DataStream<String> result  = textStream.map(s -> (index++) + ".您输入的是：" + s);
//        result.print();

        //这里的输出类型是String，所以DataStream泛型是String
        DataStream<Tuple2<String, Integer>> result2 = textStream.flatMap(new FlatMapFunction<String, Tuple2<String, Integer>>() {
            /**
             * 这里是将字符串按字符串分组，再统计字符串的数量输出
             * @param s            输入类型 string
             * @param collector      输出流   输出类型 Tuple2<String, Integer>， Tuple2是flink定义的自定义类型，当参数有N个时，就用TopleN,目前最多22个
             * @throws Exception
             */
            @Override
            public void flatMap(String s, Collector<Tuple2<String, Integer>> collector) throws Exception {
                String[] splits = s.toLowerCase().split("\\W+");
                for (String split : splits) {
                    if (split.length() > 0) {
                        collector.collect(new Tuple2<>(split, 1));
                    }
                }
            }
        }).returns(Types.TUPLE(Types.STRING, Types.INT)).keyBy(0)           //根据Tuple2的f0字段分组
                .sum(1);    //根据Tuple2的f1字段累加
        result2.print();
        env.execute("WordCount： ");
    }
}
