package com.shrek.test.flink.client;

import com.shrek.test.flink.util.Property;
import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer011;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.Producer;

import java.util.HashMap;

/**
 * TODO
 *
 * @author WuShu
 * @date 2020-05-21 17:09
 * @remark
 */
public class KafkaClient {
    private static final String GROUP = "wushu_group";
    //flink的kafka客户端
//    private static FlinkKafkaConsumer011 flinkKafkaConsumer011 = null;
    private static HashMap<String,FlinkKafkaConsumer011> KafkaCountMap = new HashMap<>();

    //得到一个kafka生产者
    private static Producer<String, String> producer = null;

    public static FlinkKafkaConsumer011 getFlinkKafkaConsumer011(String topic){
        if(! KafkaCountMap.containsKey(topic)){
            KafkaCountMap.put(topic,new FlinkKafkaConsumer011<String>(topic, new SimpleStringSchema(), Property.getKafkaProperties(GROUP)));
        }
        return KafkaCountMap.get(topic);
    }

    public static Producer<String, String> getProducer(){
        if(producer==null){
            producer = new KafkaProducer<>(Property.getKafkaProperties(GROUP));
        }
        return producer;
    }

}
