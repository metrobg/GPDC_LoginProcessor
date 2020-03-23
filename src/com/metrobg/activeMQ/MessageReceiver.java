
package com.metrobg.activeMQ;

import org.apache.activemq.ActiveMQConnectionFactory;

import javax.jms.*;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.Enumeration;
import java.util.List;
import java.util.Properties;


public class MessageReceiver {

    // Name of the queue we will receive messages from
    private static String subject = "login";
    private static Connection dbConnection = null;

    public static void main(String[] args) throws JMSException, SQLException {
        // Getting JMS connection from the server
        // URL of the JMS server
        DriverManager.registerDriver(new oracle.jdbc.driver.OracleDriver());
        String url = "tcp://192.168.144.172:61616";
        String user = "admin";
        String password = "Ign32ORw3C4b";

        ConnectionFactory connectionFactory = new ActiveMQConnectionFactory(url);
        javax.jms.Connection connection = connectionFactory.createConnection(user, password);
        connection.start();

        // Creating session for sending messages
        Session session = connection.createSession(false, Session.AUTO_ACKNOWLEDGE);

        // Destination destination = session.createQueue(subject);
        Destination destination = session.createQueue(subject);

        dbConnection = getConnection(dbConnection);
        dbConnection.setAutoCommit(false);

        // MessageConsumer is used for receiving (consuming) messages
        MessageConsumer consumer = session.createConsumer(destination);

        List messageList = getMessageList(destination, session);
        int cnt = 0;
        Message message = null;

        // Here we receive the message.
        ConsumerMessageListener consumerListener = (new ConsumerMessageListener("Consumer", dbConnection, messageList.size()));

        System.out.println("MessageReceiver is Running.");
        while (cnt < messageList.size()) {
            message = consumer.receive();
            if (message instanceof TextMessage) {
                TextMessage textMessage = (TextMessage) message;
                System.out.println("The message is: " + textMessage.getText());
                consumerListener.processMessage(textMessage.getText(), dbConnection);
            }
            cnt++;
        }
        try {
            if (!dbConnection.isClosed()) {
                dbConnection.close();
            }
        } catch (SQLException ex) {
            ex.printStackTrace();
        }
    }

    static Connection getConnection(Connection dbConnection) throws SQLException {
        Properties prop = new Properties();
        prop.setProperty("user", "user");
        prop.setProperty("password", "12345");
        dbConnection = DriverManager.getConnection("jdbc:oracle:thin:@192.168.144.234:1521:gpdc", prop);
        return dbConnection;
    }

    static List getMessageList(Destination d, Session session) throws JMSException {

        QueueBrowser browser = session.createBrowser((Queue) d);
        Enumeration enu = browser.getEnumeration();
        List list = new ArrayList();
        while (enu.hasMoreElements()) {
            TextMessage message = (TextMessage) enu.nextElement();
            list.add(message.getText());
        }
        System.out.println("Pending Messages: " + list.size());
        return list;
    }
}
