package org.janstenpickle.flume.sinks;

/**
 * Created with IntelliJ IDEA.
 * User: chris
 * Date: 05/09/2013
 * Time: 11:22
 * To change this template use File | Settings | File Templates.
 */
public class SensuSinkConstants {
    public static final int DEFAULT_BATCH_SIZE = 100;
    public static final String BATCH_SIZE = "batchSize";
    public static final String AMQP_HOST = "amqpHost";
    public static final String AMQP_PORT = "amqpPort";
    public static final int DEFAULT_AMQP_PORT = 5672;
    public static final String AMQP_USERNAME = "amqpUsername";
    public static final String AMQP_PASSWORD = "amqpPassword";
    public static final String AMQP_QUEUE = "amqpQueue";
    public static final String DEFAULT_AMQP_QUEUE = "results";
    public static final String AMQP_VHOST = "amqpVhost";
    public static final String DEFAULT_AMQP_VHOST = "/sensu";
    public static final String AMQP_ROUTING_KEY = "amqpRoutingKey";
    public static final String DEFAULT_AMQP_ROUTING_KEY = "#";
    public static final String ALERT_LEVEL = "alertLevel";
    public static final String DEFAULT_ALERT_LEVEL = "CRITICAL";
    public static final String SENSU_HANDLERS = "sensuHandlers";
    public static final String DEFAULT_SENSU_HANDLERS = "default";
}
