package com.hiscat.bio;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.net.ServerSocket;
import java.net.Socket;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

/**
 * @author hiscat
 */
public class BioTest {
    public static void main(String[] args) throws IOException {
        ServerSocket server = new ServerSocket(9000);
        @SuppressWarnings("AlibabaThreadPoolCreation")
        ExecutorService pool = Executors.newFixedThreadPool(3);
        while (true) {
            System.out.println(Thread.currentThread().getName() + " accept");
            Socket client = server.accept();
            pool.submit(() -> handle(client));
        }
    }

    private static void handle(Socket client) {
        try {
            BufferedReader reader = new BufferedReader(new InputStreamReader(client.getInputStream()));
            String line = reader.readLine();
            System.out.printf("thread: %s, line: %s\n", Thread.currentThread().getName(), line);
        } catch (IOException e) {
            e.printStackTrace();
        }
    }
}
