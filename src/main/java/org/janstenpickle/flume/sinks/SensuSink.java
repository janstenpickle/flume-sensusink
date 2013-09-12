package org.janstenpickle.flume.sinks;

import com.google.common.base.Throwables;
import com.rabbitmq.client.Channel;
import com.rabbitmq.client.Connection;
import com.rabbitmq.client.ConnectionFactory;
import org.apache.flume.*;
import org.apache.flume.conf.Configurable;
import org.apache.flume.sink.AbstractSink;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

import static org.janstenpickle.flume.sinks.SensuSinkConstants.*;

/**
 * Created with IntelliJ IDEA.
 * User: chris
 * Date: 05/09/2013
 * Time: 11:21
 * To change this template use File | Settings | File Templates.
 */
public class SensuSink extends AbstractSink implements Configurable {

    private static final Logger LOG = LoggerFactory
            .getLogger(SensuSink.class);
    private final CounterGroup counterGroup = new CounterGroup();

    private int batchSize;

    private final ConnectionFactory factory = new ConnectionFactory();
    private Connection connection;
    private Channel amqpChannel;
    private String amqpQueue;
    private String amqpRoutingKey;
    private SensuEventSerializer sensuEventSerializer;

    @Override
    public void configure(Context context) {
        batchSize = context.getInteger(BATCH_SIZE, DEFAULT_BATCH_SIZE);
        String amqpHost = context.getString(AMQP_HOST);
        int amqpPort = context.getInteger(AMQP_PORT, DEFAULT_AMQP_PORT);
        String amqpUsername = context.getString(AMQP_USERNAME, null);
        String amqpPassword = context.getString(AMQP_PASSWORD, null);
        String amqpVhost = context.getString(AMQP_VHOST, DEFAULT_AMQP_VHOST);
        amqpQueue = context.getString(AMQP_QUEUE, DEFAULT_AMQP_QUEUE);
        amqpRoutingKey = context.getString(AMQP_ROUTING_KEY, DEFAULT_AMQP_ROUTING_KEY);

        String sensuHandlers = context.getString(SENSU_HANDLERS, DEFAULT_SENSU_HANDLERS);
        String alertLevel = context.getString(ALERT_LEVEL, DEFAULT_ALERT_LEVEL);

        List<String> sensuHandlersList = new ArrayList<String>(Arrays.asList(sensuHandlers.split(",")));

        sensuEventSerializer = new SensuEventSerializer(sensuHandlersList, alertLevel);

        factory.setHost(amqpHost);
        factory.setPort(amqpPort);
        factory.setVirtualHost(amqpVhost);
        if (amqpUsername != null) {
            factory.setUsername(amqpUsername);
            if (amqpPassword != null) {
                factory.setPassword(amqpPassword);
            }
        }
        try {
            LOG.info("Starting SensuSink { hostname: "+amqpHost+":"+amqpPort+ "}");
            connection = factory.newConnection();
            amqpChannel = connection.createChannel();
            amqpChannel.exchangeDeclare(amqpQueue, "direct");
            amqpChannel.queueDeclare(amqpQueue, false, false, false, null);
            amqpChannel.queueBind(amqpQueue, amqpQueue, amqpRoutingKey);
        } catch (IOException e) {
            LOG.error("Could not create connection to rabbitMQ ", e);
        }


    }

    @Override
    public Status process() throws EventDeliveryException {
        LOG.debug("processing...");
        Status status = Status.READY;
        org.apache.flume.Channel channel = getChannel();
        Transaction txn = channel.getTransaction();
        try {
            txn.begin();
            for (int i = 0; i < batchSize; i++) {
                Event event = channel.take();


                if (event == null) {
                    break;
                }

                LOG.debug(event.toString());

                LOG.debug("Sending serialized event to AMQP");
                amqpChannel.basicPublish(amqpQueue, amqpRoutingKey, null,
                        sensuEventSerializer.getJsonByteData(event));


            }
            txn.commit();
            counterGroup.incrementAndGet("transaction.success");
        } catch (Throwable ex) {
            try {
                txn.rollback();
                counterGroup.incrementAndGet("transaction.rollback");
            } catch (Exception ex2) {
                LOG.error(
                        "Exception in rollback. Rollback might not have been successful.",
                        ex2);
            }

            if (ex instanceof Error || ex instanceof RuntimeException) {
                LOG.error("Failed to commit transaction. Transaction rolled back.",
                        ex);
                Throwables.propagate(ex);
            } else {
                LOG.error("Failed to commit transaction. Transaction rolled back.",
                        ex);
                throw new EventDeliveryException(
                        "Failed to commit transaction. Transaction rolled back.", ex);
            }
        } finally {
            txn.close();
        }
        return status;
    }

}
