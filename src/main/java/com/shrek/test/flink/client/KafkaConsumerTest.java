package com.shrek.test.flink.client;

import java.util.Arrays;
import java.util.Properties;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.serialization.StringDeserializer;


/**
 * 
* Title: KafkaConsumerTest
* Description: 
*  kafka消费者 demo
* Version:1.0.0  
* @author pancm
* @date 2018年1月26日
 */
public class KafkaConsumerTest implements Runnable {

	public final KafkaConsumer<String, String> consumer;
	public ConsumerRecords<String, String> msgList;
	public final String topic;
	public static final String GROUPID = "flume-consumer1";

	public KafkaConsumerTest(String topicName) {
		Properties props = new Properties();
		props.put("bootstrap.servers", "193.112.132.103:6667,193.112.179.58:6667,134.175.247.27:6667");
//           props.put("bootstrap.servers", "10.0.100.129:6667");
		props.put("group.id", GROUPID);
		props.put("enable.auto.commit", "true");
		props.put("auto.commit.interval.ms", "100");
		props.put("session.timeout.ms", "50000");
		props.put("max.poll.records", 22);
		props.put("auto.offset.reset", "latest");
		props.put("key.deserializer", StringDeserializer.class.getName());
		props.put("value.deserializer", StringDeserializer.class.getName());
		this.consumer = new KafkaConsumer<String, String>(props);
		this.topic = topicName;
		this.consumer.subscribe(Arrays.asList(topic));
	}

	@Override
	public void run() {
		int messageNo = 1;
		System.out.println("---------开始消费---------");
		try {
			for (;;) {
				msgList = consumer.poll(0);
				System.out.println("开始消费kafka数据");
				if(null!=msgList&&msgList.count()>0){
					for (ConsumerRecord<String, String> record : msgList) {
						//消费2条就打印 ,但打印的数据不一定是这个规律的
//                        if(messageNo%2==0){
						System.out.println(messageNo+"=======receive: key = " + record.key() + ", value = " + record.value()+" offset==="+record.offset());
//                        }
						//当消费了10条就退出
						if(messageNo%20==0){
							break;
						}
						messageNo++;
					}
				}else{
					Thread.sleep(1000);
				}
			}
		} catch (InterruptedException e) {
			System.out.println("连接kafka集群失败"+e.getMessage());
			e.printStackTrace();
		} finally {
			consumer.close();
		}
	}
	public static void main(String args[]) {
		KafkaConsumerTest test1 = new KafkaConsumerTest("login_user");
		Thread thread1 = new Thread(test1);
		thread1.start();
	}
}
