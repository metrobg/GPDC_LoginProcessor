package com.metrobg.activeMQ;

import org.apache.activemq.ActiveMQConnectionFactory;

import javax.jms.*;

public class MessageReceiver {

    // Name of the queue we will receive messages from
    private static String subject = "JCG_QUEUE";

    public static void main(String[] args) throws JMSException {
        // Getting JMS connection from the server
        // URL of the JMS server
        String url = "tcp://192.168.144.172:61616";
        String user = "admin";
        String password = "Ign32ORw3C4b";
        ConnectionFactory connectionFactory = new ActiveMQConnectionFactory(url);
        Connection connection = connectionFactory.createConnection(user,password);
        connection.start();

        // Creating session for sending messages
        Session session = connection.createSession(false, Session.AUTO_ACKNOWLEDGE);

        // Getting the queue 'JCG_QUEUE'
        Destination destination = session.createQueue(subject);
        Destination d           = session.createTopic("annual");

        // MessageConsumer is used for receiving (consuming) messages
        MessageConsumer consumer = session.createConsumer(d);

        // Here we receive the message.
        consumer.setMessageListener(new ConsumerMessageListener("Consumer"));
       // Message message = consumer.receive();

        // We will be using TestMessage in our example. MessageProducer sent us a TextMessage
        // so we must cast to it to get access to its .getText() method.
      /*  if (message instanceof TextMessage) {
            TextMessage textMessage = (TextMessage) message;
            System.out.println("Received message '" + textMessage.getText() + "'");
            if (textMessage.getText() == "SHUTDOWN") {
                connection.close();
            }
        }
    */
        //connection.close();
    }
}
