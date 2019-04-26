package edu.buffalo.cse.cse486586.simpledynamo;

import java.io.IOException;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.net.Socket;

import static edu.buffalo.cse.cse486586.simpledynamo.SimpleDynamoProvider.generateHash;

class Client {
    private static final String TAG = "CLIENT";
    private int connectedId;
    private String hashedConnectedId;
    private Socket socket;
    private ObjectInputStream ois;
    private ObjectOutputStream oos;
    private boolean connected;
    static final int timeout = 500;

    Client(Integer remoteProcessId) throws IOException, NullPointerException {
        /* Establish the connection to server and store it in a Hashmap*/
        connectedId = remoteProcessId;
        hashedConnectedId = generateHash(remoteProcessId.toString());
        socket = null;
        socket.setSoTimeout(500);
        oos = new ObjectOutputStream(socket.getOutputStream());
        ois = new ObjectInputStream(socket.getInputStream());
        connected = true;
    }

    void writeUTF(String stringToWrite) throws IOException {
        try {
            this.oos.writeUTF(stringToWrite);
            this.oos.flush();
        } catch (IOException e){
            this.connected = false;
            throw e;
        }
    }

    String readUTF() throws Exception {
        String readString = null;
        try {
            readString = this.ois.readUTF();
        } catch (IOException e){
            this.connected = false;
            throw e;
        }
        return readString;
    }

    void sendStatus(boolean status) throws Exception {
        try {
            this.oos.writeBoolean(status);
            this.oos.flush();
        } catch (Exception e){
            this.connected = false;
        }
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
