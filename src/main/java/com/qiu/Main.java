package com.qiu;

import com.qiu.net.DBServer;

public class Main {

    /**
     * 主方法
     */
    public static void main(String[] args) {
        try {
            int port = 6379;
            String dbPath = "./minidb_data";

            if (args.length > 0) {
                port = Integer.parseInt(args[0]);
            }
            if (args.length > 1) {
                dbPath = args[1];
            }

            DBServer server = new DBServer(port, dbPath);
            Runtime.getRuntime().addShutdownHook(new Thread(server::stop));
            server.start();

        } catch (Exception e) {
            System.err.println("Failed to start server: " + e.getMessage());
            e.printStackTrace();
        }
    }
}
