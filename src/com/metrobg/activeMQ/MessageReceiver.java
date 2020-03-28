
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
    private static String subject = "GPDC-LOGIN";
    private static Connection dbConnection = null;
    private static boolean TEST = false;


    public static void main(String[] args) throws JMSException, SQLException {
        // Getting JMS connection from the server
        // URL of the JMS server
        String url = "tcp://192.168.144.172:61616";
        String user = "admin";
        String password = "Ign32ORw3C4b";
        String Oracleurl = "jdbc:oracle:thin:@192.168.60.8:1521:gpdc";
        String ip_Address = "db/168.60.8 - broker/144.172";

        DriverManager.registerDriver(new oracle.jdbc.driver.OracleDriver());
        if(TEST) {
           url        = "tcp://192.168.10.66:61616";
           Oracleurl  = "jdbc:oracle:thin:@192.168.144.234:1521:gpdc";
           user       = "admin";
           password   = "admin";
           ip_Address = "db/168.144.234 - broker/10.66";
        }
        ConnectionFactory connectionFactory = new ActiveMQConnectionFactory(url);
        javax.jms.Connection connection = connectionFactory.createConnection(user, password);
        connection.start();

        // Creating session for sending messages
        Session session = connection.createSession(false, Session.AUTO_ACKNOWLEDGE);

        // Destination destination = session.createQueue(subject);
        Destination destination = session.createQueue(subject);


        // MessageConsumer is used for receiving (consuming) messages
        MessageConsumer consumer = session.createConsumer(destination);

        List messageList = getMessageList(destination, session);
        int cnt = 0;
        Message message = null;
        if(TEST) {
            System.out.println("**** TEST MODE!  TEST!  TEST!  TEST!  TEST! ****");
        }
        System.out.println("MessageReceiver is Running. host: " + ip_Address);

        // System.exit(1);

        dbConnection = getConnection(dbConnection,Oracleurl);
        dbConnection.setAutoCommit(false);

        // Here we receive the message.
        ConsumerMessageListener consumerListener = (new ConsumerMessageListener("GPDC_Processor", dbConnection, 1));
        consumer.setMessageListener(consumerListener);   //mbg

    }

    static Connection getConnection(Connection dbConnection,String jdbcURL) throws SQLException {
        try {
            Properties prop = new Properties();
            prop.setProperty("user", "develope");
            prop.setProperty("password", "merlin");
            dbConnection = DriverManager.getConnection(jdbcURL, prop);
            return dbConnection;
        } catch (SQLException e) {
            e.printStackTrace();
            System.out.println("unable to connect to Oracle");
        }
        return null;
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
