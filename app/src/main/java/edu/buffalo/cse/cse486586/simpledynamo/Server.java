package edu.buffalo.cse.cse486586.simpledynamo;

import android.util.Log;

import java.io.IOException;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.net.ServerSocket;
import java.net.Socket;

class Server extends Thread {
    static final int SERVER_PORT = 10000;
    static final String TAG = "SERVER_TASK";

    /* https://stackoverflow.com/questions/10131377/socket-programming-multiple-client-to-one-server*/

    public void run() {
        /* Open a socket at SERVER_PORT */
        ServerSocket serverSocket = null;
        try {
            serverSocket = new ServerSocket(SERVER_PORT);
            Log.d(TAG, "Server started Listening");
        } catch (IOException e) {
            e.printStackTrace();
        }

        /* Accept a client connection and spawn a thread to respond */
        while (true) {
            try {
                Socket socket = serverSocket.accept();
                Log.d(TAG, "Incoming connection....");
                new ServerThread(socket).start();
            } catch (IOException e) {
                e.printStackTrace();
            }
        }
    }
}

class ServerThread extends Thread {
    ObjectOutputStream oos;
    ObjectInputStream ois;

    static final String TAG = "SERVER_THREAD";

    public ServerThread(Socket socket) {
        try {
            this.ois = new ObjectInputStream(socket.getInputStream());
            this.oos = new ObjectOutputStream(socket.getOutputStream());
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    @Override
    public void run() {
        //Read from the socket
        while (true) {
            Request request = null;
            try {
                request = new Request(ois.readUTF());
            } catch (IOException e) {
                e.printStackTrace();
            }
            switch (request.getRequestType()) {
                default:
                    Log.d(TAG, "Unknown Operation. :-?");
                    return;
            }
        }

    }
}
