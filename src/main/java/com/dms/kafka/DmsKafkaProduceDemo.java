package com.dms.kafka;

import java.io.IOException;
import java.util.Date;
import java.util.Properties;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Future;

import org.apache.kafka.clients.producer.Callback;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;

@RestController
@RequestMapping("DmsKafkaProduce")
public class DmsKafkaProduceDemo
{
	public Logger logger = LoggerFactory.getLogger(DmsKafkaConsumeDemo.class);
	@RequestMapping("runKafkaProduce")
    public  void runKafkaProduce() throws IOException
    {
        Properties producerConfig = Config.getProducerConfig();

        producerConfig.put("ssl.truststore.location", Config.getTrustStorePath());
        System.setProperty("java.security.auth.login.config", Config.getSaslConfig());

        Producer<String, String> producer = new KafkaProducer<>(producerConfig);
        for (int i = 0; i < 10; i++)
        {
        	int partition=0;
        	String msg="hello, dms kafka.,this is msg:"+i;
        	if(i%2==0){
        		partition=0;
        	}else{
        		partition=1;
        	}
            Future<RecordMetadata> future =
                producer.send(new ProducerRecord<String, String>(
                        producerConfig.getProperty("topic"),
                        partition,partition+"", msg));
            RecordMetadata rm;
            try
            {
                rm = future.get();
                logger.info("Succeed to send msg: " + msg);
            }
            catch (InterruptedException | ExecutionException e)
            {
                e.printStackTrace();
            }
        }
        producer.close();
    }
	
	@RequestMapping("runKafkaProduceSynch")
    public  void runKafkaProduceSynch() throws IOException
    {
        Properties producerConfig = Config.getProducerConfig();

        producerConfig.put("ssl.truststore.location", Config.getTrustStorePath());
        System.setProperty("java.security.auth.login.config", Config.getSaslConfig());

        Producer<String, String> producer = new KafkaProducer<>(producerConfig);
        logger.info("start runKafkaProduceSynch sendMsg:"+new Date());
        for (int i = 0; i < 10000; i++)
        {
        	int partition=0;
        	String msg="hello, dms kafka.,this is synchMsg:"+i;
        	if(i%2==0){
        		partition=0;
        	}else{
        		partition=1;
        	}
            Future<RecordMetadata> future =
                producer.send(new ProducerRecord<String, String>(
                        producerConfig.getProperty("topic"),
                        partition,partition+"", msg));
            RecordMetadata rm;
            try
            {
                rm = future.get();
                logger.info("Succeed to send msg: " + msg);
            }
            catch (Exception e)
            {
            	logger.info(e.getMessage());
                e.printStackTrace();
            }
        }
        logger.info("end runKafkaProduceSynch sendMsg:"+new Date());
        producer.close();
    }
	
	@RequestMapping("runKafkaProduceAsynch")
    public  void runKafkaProduceAsynch() throws IOException
    {
        Properties producerConfig = Config.getProducerConfig();

        producerConfig.put("ssl.truststore.location", Config.getTrustStorePath());
        System.setProperty("java.security.auth.login.config", Config.getSaslConfig());
        Producer<String, String> producer = new KafkaProducer<>(producerConfig);
        logger.info("start runKafkaProduceAsynch sendMsg:"+new Date());
        for (int i = 0; i < 10000; i++)
        {
        	int partition=0;
        	String msg="hello, dms kafka.,this is asynchMsg:"+i;
        	if(i%2==0){
        		partition=0;
        	}else{
        		partition=1;
        	}
            producer.send(new ProducerRecord<String, String>(
                    producerConfig.getProperty("topic"),
                    partition,partition+"", msg),new Callback() {
						@Override
						public void onCompletion(RecordMetadata metadata, Exception e) {
							 //如果 Kafka 返回一个错误，onCompletion 方法会抛出一个非空（non null）异常
		            	    if (e!= null) {
		            	    	logger.info(e.getMessage());
		            	    	e.printStackTrace(); 
		            	    }
						}
            });
        }
        logger.info("end runKafkaProduceAsynch sendMsg:"+new Date());
        producer.close();
    }
	
}
