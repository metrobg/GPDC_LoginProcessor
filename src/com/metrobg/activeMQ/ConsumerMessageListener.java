package com.metrobg.activeMQ;

import javax.jms.*;
import java.sql.*;
import java.sql.Connection;
import java.util.*;

public class ConsumerMessageListener implements MessageListener {
    private Connection dbConnection;
    private long messageCount;
    private String consumerName;

    public ConsumerMessageListener(String consumerName, Connection dbConnection, long kount) {
        this.dbConnection = dbConnection;
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
                    processMessage(textMessage.getText(), dbConnection);
                    message.acknowledge();     // acknowledge message after processing
                }
                cnt++;
            }

        } catch (Exception e) {
            System.out.println("Exception caught");
        }
    }

    public void processMessage(String message, Connection dbConnection) throws SQLException {

        HashMap<String, String> payload;
        payload = new HashMap();

        String[] keyVals = message.split(";");
        for (String keyVal : keyVals) {
            String[] parts = keyVal.split("=", 2);
            payload.put(parts[0], parts[1]);
        }
        try {
            if ((dbConnection == null) || (dbConnection.isClosed())) {
                dbConnection = MessageReceiver.getConnection();
            }
            String insertStatement = "insert into GPDC_DOMAIN_LOGIN (USER_NAME,REMOTE_HOST,LOCAL_HOST,DATE_TIME,IN_OUT) " +
                    "VALUES(?,?,?,to_date(?,'mm/dd/yyyy HH:mi:ss AM'),?)";
            PreparedStatement ps = dbConnection.prepareStatement(insertStatement);


            if (payload.get("RemoteHost").contains("'")) {           // macs are using a single quote as part of the machine name
                payload.put("RemoteHost", payload.get("RemoteHost") + " " + payload.get("in_out"));
                ps.setString(5, "IN");
            }
            ps.setString(1, payload.get("User"));
            if (payload.get("RemoteHost").toUpperCase().equals("OUT")) {
                ps.setString(2, "LOGOUT");
                ps.setString(5, "OUT");
            } else {
                ps.setString(2, payload.get("RemoteHost"));
                ps.setString(5, "IN");
            }

            ps.setString(3, payload.get("ComputerName"));
            ps.setString(4, payload.get("TimeStamp"));

            ps.execute();
            System.out.println("Record inserted");
            dbConnection.commit();
            ps.close();
        } catch (SQLException e) {
            e.printStackTrace();
            System.exit(1); // shutdown if errors inserting message not acknowledged and will remain at broker
        } finally {
            this.dbConnection.close();
        }
    }

}
