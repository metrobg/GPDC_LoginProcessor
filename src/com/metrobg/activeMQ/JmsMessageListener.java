package com.metrobg.activeMQ;

// https://examples.javacodegeeks.com/enterprise-java/jms/jms-messagelistener-example/

import org.apache.activemq.ActiveMQConnectionFactory;

import javax.jms.*;
import java.text.DateFormat;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Date;
import java.util.Enumeration;
import java.util.List;


public class JmsMessageListener {
    /*private static String user = "admin";
    private static String password = "Ign32ORw3C4b";
    private static String url = "tcp://192.168.144.172:61616";*/
    private static String user = "admin";
    private static String password = "admin";
    private static String url = "tcp://192.168.10.66:61616";

    public static void main(String[] args) throws Exception {
        boolean ProducerOnly = true;
        boolean listMessagesOnly = false;
        boolean acknowledgeMessages = true;
        boolean processAllMessages = true;


        ConnectionFactory connectionFactory = new ActiveMQConnectionFactory(url);
        Connection connection = connectionFactory.createConnection(user, password);
        Session session = connection.createSession(false, Session.CLIENT_ACKNOWLEDGE);

        Date todaysDate = new Date();
        DateFormat dateFormat = new SimpleDateFormat("MM/dd/yyyy hh:mm:ss a");
        String strDate = dateFormat.format(todaysDate);

        //System.exit(1);
        try {
            // Producer
            if (ProducerOnly) {


                Queue queue = session.createQueue("TestQueue");
                String payload;
                MessageProducer producer = session.createProducer(queue);
                // send multiple test messages
                for (int i = 1; i <= 1; i++) {
                    //  payload = "User=testUser-" + i + ";RemoteHost=TestUser's;ComputerName=GPDC-TERM;TimeStamp=" + strDate + ";in_out=IPAD";
                    payload = "User=MARICELA;RemoteHost=Maricela’s;ComputerName=GPDC-TERM2;TimeStamp=" + strDate + ";in_out=MACBO";

                    Message msg = session.createTextMessage(payload);
                    System.out.println("Sending text '" + payload + "'");
                    producer.send(msg);
                }
            }
            if (!ProducerOnly) {
                Destination destination = session.createQueue("TestQueue");
                MessageConsumer consumer = session.createConsumer(destination);
                int numberToProcess = 1;

                connection.start();

                List<String> messageList = getMessageList(destination, session);
                var kount = messageList.size();
                if (processAllMessages) {
                    numberToProcess = kount;
                }
                if (listMessagesOnly) {
                    for (var i = 0; i < kount; i++) {
                        System.out.println("Message " + i + " " + messageList.get(i));
                    }
                } else {
                    // Consumer
                    for (int i = 0; i <= (numberToProcess - 1); i++) {
                        Message message = consumer.receive(1000);
                        if (message instanceof TextMessage) {
                            TextMessage textMessage = (TextMessage) message;
                            System.out.println("Payload: " + textMessage.getText());
                            if (acknowledgeMessages) {
                                System.out.println("Acknowledged message " + i);
                                message.acknowledge();
                            } else {
                                System.out.println("Processed message " + i);
                            }
                        }
                    }
                }
            }
        } finally {
            if (connection != null) {
                connection.close();
                System.out.println("Connection Closed");
            }

        }
    }

    static List<String> getMessageList(Destination d, Session session) throws JMSException {

        QueueBrowser browser = session.createBrowser((Queue) d);
        Enumeration enu = browser.getEnumeration();
        List<String> list = new ArrayList<>();
        while (enu.hasMoreElements()) {
            TextMessage message = (TextMessage) enu.nextElement();
            list.add(message.getText());
        }
        System.out.println("Pending Messages: " + list.size());
        return list;
    }

}
