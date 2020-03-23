package com.metrobg.activeMQ;

import javax.jms.*;
import java.sql.*;
import java.sql.Connection;
import java.util.*;

public class ConsumerMessageListener implements MessageListener {
    private Connection conn;
    private long messageCount;


    public ConsumerMessageListener(String consumerName, Connection dbConnection, long kount) {
        this.conn = dbConnection;
        this.messageCount = kount;

    }

    public void onMessage(Message message) {
        TextMessage textMessage = (TextMessage) message;
        int cnt = 0;
        try {
            while (cnt < messageCount) {
                // System.out.println(consumerName + " received " + textMessage.getText());
                processMessage(textMessage.getText(), conn);
                cnt++;
            }
            conn.close();
            System.out.println("Connection closed.");
        } catch (Exception e) {
            e.printStackTrace();
            try {
                if (!conn.isClosed()) {
                    conn.close();
                }
            } catch (SQLException ex) {
                ex.printStackTrace();
            } finally {
                try {
                    if (!conn.isClosed()) {
                        conn.close();
                    }
                } catch (SQLException ex) {
                    ex.printStackTrace();
                }
            }
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
            String insertStatement = "insert into GPDC_DOMAIN_LOGIN (USER_NAME,REMOTE_HOST,LOCAL_HOST,DATE_TIME) " +
                    "VALUES(?,?,?,to_date(?,'mm/dd/yyyy HH:mi:ss AM'))";
            PreparedStatement ps = dbConnection.prepareStatement(insertStatement);

            ps.setString(1, holder.get("User"));
            ps.setString(2, holder.get("RemoteHost"));
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
