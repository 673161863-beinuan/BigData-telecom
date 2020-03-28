package com.atguigu.ct.consumer.bean;

import com.atguigu.ct.common.bean.Consumer;
import com.atguigu.ct.common.constant.Names;
import com.atguigu.ct.consumer.dao.HbaseDao;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;

import java.io.IOException;
import java.util.Arrays;
import java.util.Properties;

/**
 * 通话日志消费
 */
public class CalllogConsumer implements Consumer {

    /**
     * 消费数据
     */
    @Override
    public void consume() {

        try {

            Properties pro = new Properties();
            pro.load(Thread.currentThread().getContextClassLoader().getResourceAsStream("consumer.properties"));
            KafkaConsumer<String, String> consumer = new KafkaConsumer<String, String>(pro);
            //关注主题
            consumer.subscribe(Arrays.asList(Names.TOPIC.getValue()));
            HbaseDao hdao = new HbaseDao();
            hdao.init();
            //消费数据
            while (true) {
                ConsumerRecords<String, String> consumerRecords = consumer.poll(100);
                for (ConsumerRecord<String, String> consumerRecord : consumerRecords) {
                    System.out.println(consumerRecord.value());
                    hdao.insertData(consumerRecord.value());
                  Calllog log = new Calllog(consumerRecord.value());
                    //hdao.insertData(log);
                }
            }
        } catch (Exception e) {
            e.printStackTrace();
        }
    }
    /**
     * 关闭资源
     *
     * @throws IOException
     */
    @Override
    public void close() throws IOException {
    	
    }
}
