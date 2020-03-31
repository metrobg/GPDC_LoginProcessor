package com.metrobg.activeMQ;

// https://examples.javacodegeeks.com/enterprise-java/jms/jms-messagelistener-example/

import org.apache.activemq.ActiveMQConnectionFactory;

import javax.jms.*;

public class JmsMessageListener {
    /*private static String user = "admin";
    private static String password = "Ign32ORw3C4b";
    private static String url = "tcp://192.168.144.172:61616";*/
    private static String user = "admin";
    private static String password = "admin";
    private static String url = "tcp://192.168.10.66:61616";

    public static void main(String[] args) throws Exception {
        boolean PRODUCE = false;

        Connection connection = null;
        ConnectionFactory connectionFactory = new ActiveMQConnectionFactory(url);
        connection = connectionFactory.createConnection(user, password);
        Session session = connection.createSession(false, Session.CLIENT_ACKNOWLEDGE);

        try {
            // Producer
            if (PRODUCE) {


                Queue queue = session.createQueue("TestQueue");
                String payload = null;
                String user = "user";
                MessageProducer producer = session.createProducer(queue);
                // send multiple test messages
                for (int i = 1; i <= 5; i++) {
                    payload = "User=anatasha-" + i + ";RemoteHost=OUT;ComputerName=GPDC-TERM;TimeStamp=3/26/2020 1:12:38 PM;in_out=IN";
                    Message msg = session.createTextMessage(payload);
                    System.out.println("Sending text '" + payload + "'");
                    producer.send(msg);
                }
            }
            if (!PRODUCE) {
                connection.start();
                // Consumer
                Destination destination = session.createQueue("TestQueue");
                MessageConsumer consumer = session.createConsumer(destination);
                Message message = consumer.receive(1000);
                if (message instanceof TextMessage) {
                    TextMessage textMessage = (TextMessage) message;
                    System.out.println("Payload: " + textMessage.getText() + "  ");
                    //message.acknowledge();

                }
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
