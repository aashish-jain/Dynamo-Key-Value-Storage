package edu.buffalo.cse.cse486586.simpledynamo;

import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.net.InetAddress;
import java.net.Socket;

class Client {
    private Socket socket;
    private ObjectInputStream ois;
    private ObjectOutputStream oos;
    static final int timeout = 500;

    Client(Integer remoteProcessId) throws Exception {
        /* Establish the connection to server and store it in a HashMap*/
        socket = null;
        socket = new Socket(InetAddress.getByAddress(new byte[]{10, 0, 2, 2}),
                remoteProcessId * 2);
        socket.setSoTimeout(timeout);
        oos = new ObjectOutputStream(socket.getOutputStream());
        ois = new ObjectInputStream(socket.getInputStream());
    }

    public void writeUTF(String stringToWrite) throws Exception {
        oos.writeUTF(stringToWrite);
        oos.flush();
    }

    public String readUTF() throws Exception {
        String readString;
        readString = ois.readUTF();
        return readString;
    }

}
