package edu.buffalo.cse.cse486586.simpledynamo;

import java.io.IOException;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.net.InetAddress;
import java.net.Socket;

class Client {
    private static final String TAG = "CLIENT";
    private int connectedId;
    private Socket socket;
    private ObjectInputStream ois;
    private ObjectOutputStream oos;
    static final int timeout = 1000;

    Client(Integer remoteProcessId) throws IOException, NullPointerException {
        /* Establish the connection to server and store it in a Hashmap*/
        connectedId = remoteProcessId;
        socket = null;
        socket = new Socket(InetAddress.getByAddress(new byte[]{10, 0, 2, 2}),
                remoteProcessId * 2);
        socket.setSoTimeout(timeout);
        oos = new ObjectOutputStream(socket.getOutputStream());
        ois = new ObjectInputStream(socket.getInputStream());
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
}
