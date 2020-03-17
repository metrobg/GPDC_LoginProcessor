package com.metrobg.activeMQ;

// https://examples.javacodegeeks.com/enterprise-java/jms/jms-messagelistener-example/

import java.net.URI;
import java.net.URISyntaxException;

import javax.jms.*;

import org.apache.activemq.ActiveMQConnectionFactory;
import org.apache.activemq.advisory.ConsumerListener;
import org.apache.activemq.broker.BrokerFactory;
import org.apache.activemq.broker.BrokerService;

public class JmsMessageListener {
    private static String user = "admin";
    private static String password = "Ign32ORw3C4b";
    private static String url = "tcp://192.168.144.172:61616";

    public static void main(String[] args) throws Exception {
       /*BrokerService broker = BrokerFactory.createBroker(new URI(
                "broker:(tcp://localhost:61616)"));
        broker.start();  */
        Connection connection = null;
        try {
            // Producer

            ConnectionFactory connectionFactory = new ActiveMQConnectionFactory(url);

            connection = connectionFactory.createConnection(user,password);
            Session session = connection.createSession(false,Session.AUTO_ACKNOWLEDGE);
            /*
            Queue queue = session.createQueue("customerQueue");
            String payload = "Important Task";
            Message msg = session.createTextMessage(payload);
            MessageProducer producer = session.createProducer(queue);
            System.out.println("Sending text '" + payload + "'");
            producer.send(msg);
        */

            // Consumer
            Destination destination = session.createTopic("annual");
            MessageConsumer consumer = session.createConsumer(destination);

            consumer.setMessageListener(new ConsumerMessageListener("Consumer"));
            connection.start();
            Thread.sleep(1000);
            session.close();
        } finally {
            if (connection != null) {
                connection.close();
            }
            //broker.stop();
        }
    }

}
