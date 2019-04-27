package edu.buffalo.cse.cse486586.simpledynamo;

import android.content.ContentProvider;
import android.content.ContentValues;
import android.content.Context;
import android.database.Cursor;
import android.database.sqlite.SQLiteDatabase;
import android.net.Uri;
import android.telephony.TelephonyManager;
import android.util.Log;

import java.io.IOException;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.net.ServerSocket;
import java.net.Socket;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Comparator;
import java.util.HashMap;
import java.util.List;
import java.util.TreeMap;

import static edu.buffalo.cse.cse486586.simpledynamo.Utils.contentValuesFromRequest;
import static edu.buffalo.cse.cse486586.simpledynamo.Utils.cursorFromString;
import static edu.buffalo.cse.cse486586.simpledynamo.Utils.cursorToString;
import static edu.buffalo.cse.cse486586.simpledynamo.Utils.generateHash;

public class SimpleDynamoProvider extends ContentProvider {
    static final Integer[] remotePorts = {5554, 5556, 5558, 5560, 5562};
    static final int selfProcessIdLen = 4, replicas = 3;
    static final String[] projection = new String[]{
            KeyValueStorageContract.KeyValueEntry.COLUMN_KEY,
            KeyValueStorageContract.KeyValueEntry.COLUMN_VALUE
    };

    static final String DELETE = "DELETE", CREATE = "CREATE", REQUEST = "REQUEST",
            INSERT = "INSERT", INSERTED = "INSERTED", QUERY = "QUERY",
            QUERIED = "QUERIED", SEND_REP = "SEND/REP", CONNECTION = "CONNECTION";

    private KeyValueStorageDBHelper dbHelper;
    private SQLiteDatabase dbWriter, dbReader;
    private Integer myID;
    private TreeMap<String, List<Integer>> replicaMap;
    private HashMap<Integer, Client> clientMap;

    /* Returns a unique process id using the Android telephony service*/
    private int getProcessId() {
        /* https://stackoverflow.com/questions/10115533/how-to-getsystemservice-within-a-contentprovider-in-android */
        String telephoneNumber =
                ((TelephonyManager) getContext().getSystemService(Context.TELEPHONY_SERVICE)).getLine1Number();
        int length = telephoneNumber.length();
        telephoneNumber = telephoneNumber.substring(length - selfProcessIdLen);
        int id = Integer.parseInt(telephoneNumber);
        return id;
    }

    /* Returns all the nodes where the key is stored*/
    private List<Integer> getReplicaList(String key) {
        List<Integer> remoteList = null;
        try {
            remoteList = replicaMap.ceilingEntry(key)
                    .getValue();
        } catch (NullPointerException e) {
            remoteList = replicaMap.firstEntry().getValue();
        }
        return remoteList;
    }

    private void attemptConnection(int remoteToContact) {
        Client client = clientMap.get(remoteToContact);
        if (client == null) {
            try {
                clientMap.put(remoteToContact, new Client(remoteToContact));
            } catch (IOException e) {
                Log.e(CONNECTION, "Unable to connect to remote " + remoteToContact);
            }
        }
    }

    /* Returns the type of the given uri (Always String)*/
    @Override
    public String getType(Uri uri) {
        return "String";
    }

    /* Initializes the */
    private void initializeDynamoTreeMap() {
        /* Sort the array */
        Arrays.sort(remotePorts, new Comparator<Integer>() {
            @Override
            public int compare(Integer lhs, Integer rhs) {
                return generateHash(lhs.toString()).compareTo(generateHash(rhs.toString()));
            }
        });

        /* Create the TreeMap with all the AVDs to be contacted for a given key */
        /* Also try connecting to the sockets */
        ArrayList<Integer> remoteList;
        int numRemotes = remotePorts.length;
        for (int i = 0; i < remotePorts.length; i++) {
            remoteList = new ArrayList<Integer>(replicas);

            Client client = null;
            try {
                client = new Client(SimpleDynamoProvider.remotePorts[i]);
            } catch (Exception e) {
                Log.e(CREATE, "Unable to connect to remote " + SimpleDynamoProvider.remotePorts[i]);
            }

            if (client != null)
                Log.d(CREATE, "Connected to remote " + SimpleDynamoProvider.remotePorts[i]);

            clientMap.put(SimpleDynamoProvider.remotePorts[i], client);

            /* Add all the AVDs until end of ring */
            for (int j = i; remoteList.size() < replicas; j++) {
                remoteList.add(remotePorts[j % numRemotes]);
            }

            replicaMap.put(generateHash(remotePorts[i].toString()), remoteList);
        }
    }

    /* Sends a given request to all replicas in the dynamo ring  for given key */
    public synchronized boolean sendAllReplicas(final Request request) throws Exception {
        List<Integer> remoteList = getReplicaList(request.getHashedKey());
        Log.d(SEND_REP, "Remote list is " + remoteList);

        for (Integer remoteToContact : remoteList) {
            attemptConnection(remoteToContact);
            Client client = clientMap.get(remoteToContact);
            client.writeUTF(request.toString());
            Log.d(SEND_REP, "Inserted " + request.toString() + " at " + remoteToContact);
        }
        return true;
    }

    public synchronized String sendAndGetAll(final Request request) throws Exception {
        List<Integer> remoteList = getReplicaList(request.getHashedKey());
        StringBuilder stringBuilder = new StringBuilder();
        for (Integer remoteToContact : remotePorts) {
            attemptConnection(remoteToContact);
            Client client = clientMap.get(remoteToContact);
            Log.d(REQUEST, "ASKED " + remoteToContact + " for value" + request.getRequestType());
            client.writeUTF(request.toString());
            Log.d(REQUEST, " Sent !!!");
            String response = client.readUTF();
            Log.d(REQUEST, "Response " + response);
            stringBuilder.append(response);
        }
        return stringBuilder.toString();
    }

    public synchronized String sendAndGet(final Request request) throws Exception {
        List<Integer> remoteList = getReplicaList(request.getHashedKey());
        int remoteToContact = remoteList.get(0);
        attemptConnection(remoteToContact);
        Client client = clientMap.get(remoteToContact);
        Log.d(REQUEST, "ASKED " + remoteToContact + " for value");
        client.writeUTF(request.toString());
        Log.d(REQUEST, " Sent !!!");
        String response = client.readUTF();
        Log.d(REQUEST, "Response " + response);
        return response;
    }


    @Override
    public boolean onCreate() {
        /* All initializations */
        dbHelper = new KeyValueStorageDBHelper(getContext());
        dbWriter = dbHelper.getWritableDatabase();
        dbReader = dbHelper.getReadableDatabase();
        myID = getProcessId();

        this.replicaMap = new TreeMap<String, List<Integer>>();
        this.clientMap = new HashMap<Integer, Client>(remotePorts.length);

        Log.d(CREATE, "id is " + myID + " ");

        /* Starts the new server thread and wait for it to start*/
        Thread serverThread = new Server();
        serverThread.start();
        try {
            Log.d(CREATE, "Waiting for server to start");
            synchronized (serverThread) {
                serverThread.wait();
            }
        } catch (InterruptedException e) {
            e.printStackTrace();
        }

        /* Start the clients */
        initializeDynamoTreeMap();

        return true;
    }

    public long insertLocal(ContentValues values) {
        Log.d(INSERTED, values.toString());
        return dbWriter.insertWithOnConflict(KeyValueStorageContract.KeyValueEntry.TABLE_NAME,
                null, values, SQLiteDatabase.CONFLICT_REPLACE);
    }

    @Override
    public Uri insert(Uri uri, ContentValues values) {
        Log.d(INSERT, values.toString());
        String key = values.getAsString("key");
        try {
            sendAllReplicas(new Request(myID, values, RequestType.INSERT));
        } catch (Exception e) {
            e.printStackTrace();
        }
        return uri;
    }

    @Override
    public int update(Uri uri, ContentValues values, String selection,
                      String[] selectionArgs) {
        // TODO Auto-generated method stub
        return 0;
    }

    public Cursor queryLocal(String key) {
        Log.d(QUERIED, "Querying " + key);
        String[] selectionArgs = new String[]{key};
        String selection = KeyValueStorageContract.KeyValueEntry.COLUMN_KEY + " = ?";

        /* https://developer.android.com/training/data-storage/sqlite */
        Cursor cursor = dbReader.query(KeyValueStorageContract.KeyValueEntry.TABLE_NAME, this.projection,
                selection, selectionArgs, null, null, null);
        return cursor;
    }

    public Cursor queryAllLocal() {
        //Query everything
        return dbReader.query(KeyValueStorageContract.KeyValueEntry.TABLE_NAME, this.projection,
                null, null, null, null,
                null, null);
    }


    @Override
    public Cursor query(Uri uri, String[] projection, String selection,
                        String[] selectionArgs, String sortOrder) {
        Log.d(QUERY, "Querying " + selection);
        Cursor cursor = null;
        if (selection.equals("@")) {
            cursor = queryAllLocal();
        } else if (selection.equals("*")) {
            try {
                cursor = cursorFromString(sendAndGetAll(new Request(myID, selection, null, RequestType.QUERY_ALL)));
                Log.d("GOT",cursorToString(cursor));
            } catch (Exception e) {
                e.printStackTrace();
            }
        } else {
            Request queryRequest = new Request(myID, selection, null, RequestType.QUERY);
            try {
                cursor = cursorFromString(sendAndGet(queryRequest));
            } catch (IOException e) {
                e.printStackTrace();
            } catch (Exception e) {
                e.printStackTrace();
            }
        }
        if (cursor != null && cursor.getCount() == 0)
            Log.e(QUERY, "No values found :-(");
        return cursor;
    }


    @Override
    public int delete(Uri uri, String selection, String[] selectionArgs) {
        // TODO Auto-generated method stub
        return 0;
    }


    private class Server extends Thread {
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

            synchronized (this) {
                notify();
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


    private class ServerThread extends Thread {
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

        private void sendReponse(Cursor cursor) throws IOException {
            oos.writeUTF(cursorToString(cursor));
            oos.flush();
        }

        @Override
        public void run() {
            //Read from the socket
            try {
                Cursor cursor = null;
                Request request = null;
                while (true) {
                    request = new Request(ois.readUTF());
                    Log.d(TAG, request.toString());
                    switch (request.getRequestType()) {
                        case INSERT:
                            insertLocal(contentValuesFromRequest(request));
                            break;
                        case QUERY:
                            cursor = queryLocal(request.getKey());
                            Log.d(TAG, "fetched Cursor " + cursorToString(cursor));
                            sendReponse(cursor);
                            break;
                        case QUERY_ALL:
                            cursor = queryAllLocal();
                            Log.d(TAG, "fetched Cursor " + cursorToString(cursor));
                            sendReponse(cursor);
                            break;
                        case DELETE:
                            break;
                        case DELETE_ALL:
                            break;
                        case QUIT:
                            break;
                        default:
                            Log.d(TAG, "Unknown Operation. :-?");
                            return;
                    }
                }
            } catch (IOException e) {
                e.printStackTrace();
            }
        }
    }
}
