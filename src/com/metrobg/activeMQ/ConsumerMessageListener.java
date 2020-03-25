package com.metrobg.activeMQ;

import javax.jms.*;
import java.sql.*;
import java.sql.Connection;
import java.util.*;

public class ConsumerMessageListener implements MessageListener {
    private Connection conn;
    private long messageCount;
    private String consumerName;

    public ConsumerMessageListener(String consumerName, Connection dbConnection, long kount) {
        this.conn = dbConnection;
        this.messageCount = kount;
        this.consumerName = consumerName;
    }

    @Override
    public void onMessage(Message message) {
        int cnt = 0;
        try {

            while (cnt < messageCount) {

                if (message instanceof TextMessage) {
                    TextMessage textMessage = (TextMessage) message;
                    System.out.print("Payload: " + textMessage.getText() + "  ");
                    processMessage(textMessage.getText(), conn);
                }
                cnt++;
            }


        } catch (Exception e) {
            System.out.println("Exception caught");
        }
    }

    public void processMessage(String message, Connection dbConnection) throws SQLException {

        HashMap<String, String> holder;
        holder = new HashMap();

        String[] keyVals = message.split(";");
        for (String keyVal : keyVals) {
            String[] parts = keyVal.split("=", 2);
            holder.put(parts[0], parts[1]);
        }
        try {
            String insertStatement = "insert into GPDC_DOMAIN_LOGIN (USER_NAME,REMOTE_HOST,LOCAL_HOST,DATE_TIME,IN_OUT) " +
                    "VALUES(?,?,?,to_date(?,'mm/dd/yyyy HH:mi:ss AM'),?)";
            PreparedStatement ps = dbConnection.prepareStatement(insertStatement);

            ps.setString(1, holder.get("User"));
            if (holder.get("RemoteHost").toUpperCase().equals("OUT")) {
                ps.setString(2, "LOGOUT");
                ps.setString(5, "OUT");
            } else {
                ps.setString(2, holder.get("RemoteHost"));
                ps.setString(5, holder.get("in_out"));
            }

            ps.setString(3, holder.get("ComputerName"));
            ps.setString(4, holder.get("TimeStamp"));

            ps.execute();
            System.out.println("Record inserted");
            conn.commit();

        } catch (SQLException e) {
            e.printStackTrace();
        }
    }

}
