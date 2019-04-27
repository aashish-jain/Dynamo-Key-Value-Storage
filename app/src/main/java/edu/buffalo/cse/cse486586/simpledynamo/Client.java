package edu.buffalo.cse.cse486586.simpledynamo;

import java.io.IOException;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.net.InetAddress;
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
    static final int timeout = 1000;

    Client(Integer remoteProcessId) throws IOException, NullPointerException {
        /* Establish the connection to server and store it in a Hashmap*/
        connectedId = remoteProcessId;
        hashedConnectedId = generateHash(remoteProcessId.toString());
        socket = null;
        socket = new Socket(InetAddress.getByAddress(new byte[]{10, 0, 2, 2}),
                remoteProcessId * 2);
        socket.setSoTimeout(timeout);
        oos = new ObjectOutputStream(socket.getOutputStream());
        ois = new ObjectInputStream(socket.getInputStream());
        connected = true;
        socket.setSoTimeout(0);
    }

    void writeUTF(String stringToWrite) throws Exception {
        this.oos.writeUTF(stringToWrite);
        this.oos.flush();
    }

    String readUTF() throws Exception {
        String readString = null;
        readString = this.ois.readUTF();
        return readString;
    }

    void sendStatus(boolean status) throws Exception {
        try {
            this.oos.writeBoolean(status);
            this.oos.flush();
        } catch (Exception e) {
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
