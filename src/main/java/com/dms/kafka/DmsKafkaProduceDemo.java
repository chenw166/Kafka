package com.dms.kafka;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;
import java.util.Properties;
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
        int faliCount=0;
        long start_time=System.currentTimeMillis();
        for (int i = 0; i < 100000; i++)
        {
        	int partition=0;
        	String msg="hello, dms kafka.,this is msg:"+i;
        	if(i%2==0){
        		partition=0;
        	}else{
        		partition=1;
        	}          
            //RecordMetadata rm;
            try
            {
            	producer.send(new ProducerRecord<String, String>(
                        producerConfig.getProperty("topic"),
                        partition,partition+"", msg));
                System.out.println("Succeed to send msg: " + msg);
            }
            catch (Exception e)
            {
            	faliCount++;
            	logger.info("Fail to send msg:"+msg);
            	logger.info(e.getMessage());
            	e.printStackTrace();
            }
        }
        System.out.println("Fail number: " + faliCount);
        long end_time=System.currentTimeMillis();
        System.out.println("Execute time: " + (end_time-start_time)/1000+"s");
        producer.close();
    }
	
	@RequestMapping("runKafkaProduceSynch")
    public  void runKafkaProduceSynch() throws IOException
    {
        Properties producerConfig = Config.getProducerConfig();

        producerConfig.put("ssl.truststore.location", Config.getTrustStorePath());
        System.setProperty("java.security.auth.login.config", Config.getSaslConfig());

        Producer<String, String> producer = new KafkaProducer<>(producerConfig);
        int faliCount=0;
        long start_time=System.currentTimeMillis();
        for (int i = 0; i < 100000; i++)
        {
        	int partition=0;
        	String msg="hello, dms kafka.,this is synchMsg:"+i;
        	if(i%2==0){
        		partition=0;
        	}else{
        		partition=1;
        	}
            
            try
            {
            	Future<RecordMetadata> future =
                        producer.send(new ProducerRecord<String, String>(
                                producerConfig.getProperty("topic"),
                                partition,partition+"", msg));
                    RecordMetadata rm;
            	rm = future.get();
            	System.out.println("Succeed to send msg: " + msg);
            }
            catch (Exception e)
            {
            	faliCount++;
            	logger.info(e.getMessage());
                e.printStackTrace();
            }
        }
        System.out.println("Fail number: " + faliCount);
        long end_time=System.currentTimeMillis();
        System.out.println("Execute time: " + (end_time-start_time)/1000+"s");
        producer.close();
    }
	
	@RequestMapping("runKafkaProduceAsynch")
    public  void runKafkaProduceAsynch() throws IOException
    {
        Properties producerConfig = Config.getProducerConfig();

        producerConfig.put("ssl.truststore.location", Config.getTrustStorePath());
        System.setProperty("java.security.auth.login.config", Config.getSaslConfig());
        Producer<String, String> producer = new KafkaProducer<>(producerConfig);
        int faliCount=0;
        long start_time=System.currentTimeMillis();
        
        final Map<String,Object> result=new HashMap<String,Object>();
        result.put("start_time", start_time);
        result.put("tryCount", 0);
        result.put("failCount", 0);
        for (int i = 0; i < 100000; i++)
        {
        	int partition=0;
        	String msg="hello, dms kafka.,this is asynchMsg:"+i;
        	if(i%2==0){
        		partition=0;
        	}else{
        		partition=1;
        	}
        	String key=i+"";
            try{
	        	producer.send(new ProducerRecord<String, String>(
	                    producerConfig.getProperty("topic"),
	                    partition,key, msg),new Callback() {
							@Override
							public void onCompletion(RecordMetadata metadata, Exception e) {
								//如果 Kafka 返回一个错误，onCompletion 方法会抛出一个非空（non null）异常
								//System.out.println(metadata.offset());
								int tryCount=Integer.parseInt(result.get("tryCount").toString());
								int failCount=Integer.parseInt(result.get("failCount").toString());
								tryCount++;
								result.put("tryCount",tryCount);
								System.out.println(tryCount);
								if(tryCount==100000){
									if (e!= null) {
										failCount++;
										result.put("tryCount",tryCount);
				            	    	logger.info(e.getMessage());
				            	    	e.printStackTrace(); 
				            	    }
									long start_time=Long.parseLong(result.get("start_time").toString());
									long end_time=System.currentTimeMillis();
									System.out.println("Fail to send msg: " + failCount);
									System.out.println("Execute time: " + (end_time-start_time)/1000+"s");
								}else{									
									if (e!= null) {
										failCount++;
										result.put("tryCount",tryCount);
				            	    	logger.info(e.getMessage());
				            	    	e.printStackTrace(); 
				            	    }
								}
								
							}
	            });
            }catch (Exception e)
            {
            	faliCount++;
            	logger.info(e.getMessage());
                e.printStackTrace();
            }
        }
        
        
        producer.close();
    }
	
}
