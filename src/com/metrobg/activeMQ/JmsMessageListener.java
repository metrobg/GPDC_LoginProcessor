package com.metrobg.activeMQ;

// https://examples.javacodegeeks.com/enterprise-java/jms/jms-messagelistener-example/

import org.apache.activemq.ActiveMQConnectionFactory;
import org.apache.activemq.broker.BrokerFactory;
import org.apache.activemq.broker.BrokerService;

import javax.jms.*;
import java.net.URI;

public class JmsMessageListener {
    /*private static String user = "admin";
    private static String password = "Ign32ORw3C4b";
    private static String url = "tcp://192.168.144.172:61616";*/
    private static String user = "admin";
    private static String password = "admin";
    private static String url = "tcp://192.168.10.66:61616";
    public static void main(String[] args) throws Exception {

        Connection connection = null;
        try {
            // Producer

            ConnectionFactory connectionFactory = new ActiveMQConnectionFactory(url);

            connection = connectionFactory.createConnection(user,password);
            Session session = connection.createSession(false,Session.AUTO_ACKNOWLEDGE);


            Queue queue = session.createQueue("TestQueue");
            String payload = "Important Task";
            Message msg = session.createTextMessage(payload);
            MessageProducer producer = session.createProducer(queue);
            System.out.println("Sending text '" + payload + "'");
            producer.send(msg);


            connection.start();
            // Consumer
            Destination destination = session.createQueue("TestQueue");
            MessageConsumer consumer = session.createConsumer(destination);
            Message message = consumer.receive(1000);
            if (message instanceof TextMessage) {
                TextMessage textMessage = (TextMessage) message;
                System.out.println("Payload: " + textMessage.getText() + "  ");
            }

            // consumer.setMessageListener(new ConsumerMessageListener("Consumer"));


        } finally {
            if (connection != null) {
                connection.close();
                System.out.println("Connection Closed");
            }

        }
    }

}
