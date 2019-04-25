package edu.buffalo.cse.cse486586.simpledynamo;

import java.io.IOException;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.net.InetAddress;
import java.net.Socket;

class Client {
    private static final String TAG = "CLIENT";
    private int connectedId;
    private String hashedConnectedId;
    private Socket socket;
    private ObjectInputStream ois;
    private ObjectOutputStream oos;
    boolean connected;

    Client(Integer remoteProcessId) {
        /* Establish the connection to server and store it in a Hashmap*/
        connectedId = remoteProcessId;
        hashedConnectedId = SimpleDynamoProvider.generateHash(remoteProcessId.toString());
        socket = null;
        ois = null;
        oos = null;
        connected = false;
    }

    void connect() throws IOException{
        socket = new Socket(InetAddress.getByAddress(new byte[]{10, 0, 2, 2}),
                connectedId * 2);
        socket.setSoTimeout(1000);
        oos = new ObjectOutputStream(socket.getOutputStream());
        ois = new ObjectInputStream(socket.getInputStream());
        connected = true;
    }
    void writeUTF(String stringToWrite) throws IOException {
        this.oos.writeUTF(stringToWrite);
        this.oos.flush();
    }

    String readUTF() throws IOException {
        return this.ois.readUTF();
    }

    void sendStatus(boolean status) throws IOException {
        this.oos.writeBoolean(status);
        this.oos.flush();
    }

    boolean readStatus() throws IOException {
        return this.ois.readBoolean();
    }

    void close() throws IOException {
        oos.close();
        ois.close();
        socket.close();
        connected = false;
    }

    public String getHashedId() {
        return hashedConnectedId;
    }

    public int getConnectedId() {
        return connectedId;
    }

    public boolean isConnected() {
        return connected;
    }
}
