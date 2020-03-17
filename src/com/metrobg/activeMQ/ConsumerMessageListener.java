package com.metrobg.activeMQ;

import javax.jms.*;

public class ConsumerMessageListener implements MessageListener {
    private String consumerName;
    Connection conn;

    public ConsumerMessageListener(String consumerName) {
        this.consumerName = consumerName;
       // this.conn = connection;
    }

    public void onMessage(Message message) {
        TextMessage textMessage = (TextMessage) message;
        try {
            System.out.println(consumerName + " received "   + textMessage.getText());
        } catch (JMSException e) {
            e.printStackTrace();
        }
    }

}