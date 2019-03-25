package com.dms.kafka;

import java.io.IOException;
import java.util.Arrays;
import java.util.Collection;
import java.util.Iterator;
import java.util.Properties;

import org.apache.kafka.clients.consumer.ConsumerRebalanceListener;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.TopicPartition;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;

@RestController
@RequestMapping("DmsKafkaConsume")
public class DmsKafkaConsumeDemo
{
	public Logger logger = LoggerFactory.getLogger(DmsKafkaConsumeDemo.class);
	
	
	@RequestMapping("runKafkaConsume0")
    public void runKafkaConsume0() throws IOException
    {
        Properties consumerConfig = Config.getConsumerConfig();

        consumerConfig.put("ssl.truststore.location", Config.getTrustStorePath());
        System.setProperty("java.security.auth.login.config", Config.getSaslConfig());

        KafkaConsumer<String, String> kafkaConsumer = new KafkaConsumer<>(consumerConfig);
        TopicPartition p = new TopicPartition(consumerConfig.getProperty("topic"),0);
        kafkaConsumer.assign(Arrays.asList(p));
        while(true){
            ConsumerRecords<String, String> records = kafkaConsumer.poll(60000);
            for (ConsumerRecord<String, String> record : records) {
            	String msg="Received message: (" + record.key() + ", " + record.value() + ") at partition "+record.partition()+" offset " + record.offset();
                logger.info(msg);
            }
            kafkaConsumer.commitSync();
            kafkaConsumer.close();
            break;
        }
    }
	
	@RequestMapping("runKafkaConsume1")
    public void runKafkaConsume1() throws IOException
    {
        Properties consumerConfig = Config.getConsumerConfig();

        consumerConfig.put("ssl.truststore.location", Config.getTrustStorePath());
        System.setProperty("java.security.auth.login.config", Config.getSaslConfig());

        KafkaConsumer<String, String> kafkaConsumer = new KafkaConsumer<>(consumerConfig);
        TopicPartition p = new TopicPartition(consumerConfig.getProperty("topic"),1);
        kafkaConsumer.assign(Arrays.asList(p));
        while(true){
            ConsumerRecords<String, String> records = kafkaConsumer.poll(60000);
            for (ConsumerRecord<String, String> record : records) {
            	String msg="Received message: (" + record.key() + ", " + record.value() + ") at partition "+record.partition()+" offset " + record.offset();
                logger.info(msg);
            }
            kafkaConsumer.commitSync();
            kafkaConsumer.close();
            break;
        }
    }
	
	
	@RequestMapping("runKafkaConsume")
    public void runKafkaConsume() throws IOException
    {
        Properties consumerConfig = Config.getConsumerConfig();

        consumerConfig.put("ssl.truststore.location", Config.getTrustStorePath());
        System.setProperty("java.security.auth.login.config", Config.getSaslConfig());

        KafkaConsumer<String, String> kafkaConsumer = new KafkaConsumer<>(consumerConfig);
        kafkaConsumer.subscribe(Arrays.asList(consumerConfig.getProperty("topic")),
                new ConsumerRebalanceListener()
                {
                    @Override
                    public void onPartitionsRevoked(Collection<TopicPartition> arg0)
                    {

                    }

                    @Override
                    public void onPartitionsAssigned(Collection<TopicPartition> tps)
                    {

                    }
                });

        ConsumerRecords<String, String> records = null;
        while (true)
        {
            records = kafkaConsumer.poll(200);
            if (records == null || records.count() == 0)
            {
                System.out.println("There is no message. try again.");
                continue;
            }

            Iterator<ConsumerRecord<String, String>> iter = records.iterator();
            while (iter.hasNext())
            {
                ConsumerRecord<String, String> cr = iter.next();
                System.out.println("Consume msg: " + cr.value());
            }
            kafkaConsumer.commitSync();
            kafkaConsumer.close();
            break;
        }
    }
}
