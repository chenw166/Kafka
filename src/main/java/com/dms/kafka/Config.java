package com.dms.kafka;

import java.io.IOException;
import java.util.Properties;

public class Config
{
    private static final String PRODUCE_CONFIG = "producer.properties";

    private static final String CONSUME_CONFIG = "consumer.properties";

    private static final String SASL_CONFIG = "dms_kafka_client_jaas.conf";

    private static final String TRUSTSTORE_PATH = "client.truststore.jks";

    public static Properties getProducerConfig() throws IOException
    {
        return getPropertyFromClassPath(PRODUCE_CONFIG);
    }

    public static Properties getConsumerConfig() throws IOException
    {
        return getPropertyFromClassPath(CONSUME_CONFIG);
    }

    public static String getSaslConfig() throws IOException
    {
        return getClassLoader().getResource(SASL_CONFIG).getPath();
    }

    public static String getTrustStorePath() throws IOException
    {
        return getClassLoader().getResource(TRUSTSTORE_PATH).getPath();
    }

    private static Properties getPropertyFromClassPath(String resourceName) throws IOException
    {
        ClassLoader classLoader = Thread.currentThread().getContextClassLoader();
        if (classLoader == null)
        {
            classLoader = Config.class.getClassLoader();
        }

        Properties properties = new Properties();
        properties.load(classLoader.getResourceAsStream(resourceName));
        return properties;
    }

    private static ClassLoader getClassLoader()
    {
        ClassLoader classLoader = Thread.currentThread().getContextClassLoader();
        if (classLoader == null)
        {
            classLoader = Config.class.getClassLoader();
        }
        return classLoader;
    }

}
