package edu.buffalo.cse.cse486586.simpledynamo;

import android.content.ContentProvider;
import android.content.ContentValues;
import android.content.Context;
import android.database.Cursor;
import android.database.MatrixCursor;
import android.database.sqlite.SQLiteDatabase;
import android.net.Uri;
import android.telephony.TelephonyManager;
import android.util.Log;

import java.io.IOException;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.net.ServerSocket;
import java.net.Socket;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Comparator;
import java.util.Formatter;
import java.util.HashMap;
import java.util.List;
import java.util.TreeMap;

public class SimpleDynamoProvider extends ContentProvider {
    static final int selfProcessIdLen = 4;
    static final Integer[] remotePorts = {5554, 5556, 5558, 5560, 5562};
    static final int replicas = 3;
    static final String DELETE = "DELETE", CREATE = "CREATE", REQUEST_TAG = "REQUEST",
            INSERT = "INSERT", INSERTED = "INSERTED", QUERY = "QUERY", QUERYED = "QUERIED", SEND_TO_REPLICAS = "SEND/REP";
    static final String[] projection = new String[]{
            KeyValueStorageContract.KeyValueEntry.COLUMN_KEY,
            KeyValueStorageContract.KeyValueEntry.COLUMN_VALUE
    };

    private KeyValueStorageDBHelper dbHelper;
    private SQLiteDatabase dbWriter, dbReader;
    private Integer myID;
    private String myHash;
    private TreeMap<String, List<Integer>> dynamoTreeMap;
    private HashMap<Integer, Client> clientHashMap;

    public List<Integer> getRemoteList(String key) {
        List<Integer> remoteList = null;
        try {
            remoteList = dynamoTreeMap.ceilingEntry(key)
                    .getValue();
        } catch (NullPointerException e) {
            remoteList = dynamoTreeMap.firstEntry().getValue();
        }
        return remoteList;
    }


    @Override
    public String getType(Uri uri) {
        return "string";
    }

    static String generateHash(String input) {
        Formatter formatter = new Formatter();
        try {
            MessageDigest sha1 = MessageDigest.getInstance("SHA-1");
            byte[] sha1Hash = sha1.digest(input.getBytes());
            for (byte b : sha1Hash)
                formatter.format("%02x", b);
        } catch (NoSuchAlgorithmException e) {
            Log.e("HASHER", "SHA-1 not found for encryption");
        }
        return formatter.toString();
    }

    /* https://stackoverflow.com/questions/3105080/output-values-found-in-cursor-to-logcat-android */
    private static String cursorToString(Cursor cursor) {
        StringBuilder stringBuilder = new StringBuilder();
        int cursorCount = 1;
        if (cursor.moveToFirst()) {
            do {
                int columnsQty = cursor.getColumnCount();
                for (int idx = 0; idx < columnsQty; ++idx) {
                    stringBuilder.append(cursor.getString(idx));
                    if (idx < columnsQty - 1)
                        stringBuilder.append(",");
                }
                if (cursorCount < cursor.getCount())
                    stringBuilder.append("\n");
                cursorCount++;
            } while (cursor.moveToNext());
        }
        return stringBuilder.toString();
    }

    private static Cursor cursorFromString(String queryResult) {
        MatrixCursor matrixCursor = new MatrixCursor(new String[]{"key", "value"});
        String key, value;
        String[] splitValues;
        for (String row : queryResult.split("\n")) {
            splitValues = row.split(",");
            try {
                key = splitValues[0];
                value = splitValues[1];
                matrixCursor.addRow(new String[]{key, value});
            } catch (Exception e) {
                //Do nothing or skip the exception row
            }
        }
        return matrixCursor;
    }

    private static ContentValues contentValuesFromRequest(Request request) {
        ContentValues contentValues = new ContentValues();
        contentValues.put("key", request.getKey());
        contentValues.put("value", request.getValue());
        return contentValues;
    }

    private int getProcessId() {
        /* https://stackoverflow.com/questions/10115533/how-to-getsystemservice-within-a-contentprovider-in-android */
        String telephoneNumber =
                ((TelephonyManager) getContext().getSystemService(Context.TELEPHONY_SERVICE)).getLine1Number();
        int length = telephoneNumber.length();
        telephoneNumber = telephoneNumber.substring(length - selfProcessIdLen);
        int id = Integer.parseInt(telephoneNumber);
        return id;
    }


    private  void initializeDynamoTreeMap() {
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

            clientHashMap.put(SimpleDynamoProvider.remotePorts[i], client);

            /* Add all the AVDs until end of ring */
            for (int j = i; remoteList.size() < replicas; j++) {
                remoteList.add(remotePorts[j % numRemotes]);
            }

            dynamoTreeMap.put(generateHash(remotePorts[i].toString()), remoteList);

        }
    }

    /* Sends a given request to all replicas in the dynamo ring  for given key */
    public synchronized boolean sendToAllReplicas(final Request request) throws Exception {
        List<Integer> remoteList = getRemoteList(request.getHashedKey());
        Log.d(SEND_TO_REPLICAS, "Remote list is " + remoteList);

        for (Integer remoteToContact : remoteList) {
            if (clientHashMap.get(remoteToContact) == null) {
                clientHashMap.put(remoteToContact, new Client(remoteToContact));
                Log.d(SEND_TO_REPLICAS, "Added new remote " + remoteToContact);
            }
            Client client = clientHashMap.get(remoteToContact);
            client.writeUTF(request.toString());
            Log.d(SEND_TO_REPLICAS, "Inserted " + request.toString() + " at " + remoteToContact);
        }
        return true;
    }

    public synchronized String sendToAllNodesAndWait(final Request request) throws Exception {
        List<Integer> remoteList = getRemoteList(request.getHashedKey());
        StringBuilder stringBuilder = new StringBuilder();
        for(Integer remoteToContact : remotePorts) {
            if (clientHashMap.get(remoteToContact) == null) {
                clientHashMap.put(remoteToContact, new Client(remoteToContact));
                Log.d(REQUEST_TAG, "Added new remote " + remoteToContact);
            }
            Client client = clientHashMap.get(remoteToContact);
            Log.d(REQUEST_TAG, "ASKED " + remoteList.get(remoteToContact) + " for value");
            client.writeUTF(request.toString());
            Log.d(REQUEST_TAG, " Sent !!!");
            String response = client.readUTF();
            Log.d(REQUEST_TAG, "Response " + response);
            stringBuilder.append(response);
        }
        return stringBuilder.toString();
    }

    public synchronized String sendToNodeAndWaitForResponse(final Request request, final int remoteNum) throws Exception {
        List<Integer> remoteList = getRemoteList(request.getHashedKey());
        int remoteToContact = remoteList.get(remoteNum);
        if (clientHashMap.get(remoteToContact) == null) {
            clientHashMap.put(remoteToContact, new Client(remoteToContact));
            Log.d(REQUEST_TAG, "Added new remote " + remoteToContact);
        }
        Client client = clientHashMap.get(remoteToContact);
        Log.d(REQUEST_TAG, "ASKED " + remoteList.get(remoteNum) + " for value");
        client.writeUTF(request.toString());
        Log.d(REQUEST_TAG, " Sent !!!");
        String response = client.readUTF();
        Log.d(REQUEST_TAG, "Response " + response);
        return response;
    }


    @Override
    public boolean onCreate() {
        /* All initializations */
        dbHelper = new KeyValueStorageDBHelper(getContext());
        dbWriter = dbHelper.getWritableDatabase();
        dbReader = dbHelper.getReadableDatabase();
        myID = getProcessId();

        this.dynamoTreeMap = new TreeMap<String, List<Integer>>();
        this.clientHashMap = new HashMap<Integer, Client>(remotePorts.length);

        myHash = generateHash(myID.toString());

        Log.d(CREATE, "id is " + myID + " " + myHash);

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
            sendToAllReplicas(new Request(myID, values, RequestType.INSERT));
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
        Log.d(QUERYED, "Querying " + key);
        String[] selectionArgs = new String[]{key};
        String selection = KeyValueStorageContract.KeyValueEntry.COLUMN_KEY + " = ?";

        /* https://developer.android.com/training/data-storage/sqlite */
        Cursor cursor = dbReader.query(
                KeyValueStorageContract.KeyValueEntry.TABLE_NAME,   // The table to query
                this.projection,        // The array of columns to return (pass null to get all)
                selection,              // The columns for the WHERE clause
                selectionArgs,          // The values for the WHERE clause
                null,           // don't group the rows
                null,            // don't filter by row groups
                null               // The sort order
        );

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
        }
        else if (selection.equals("*")){
            try{
             cursor = cursorFromString(sendToAllNodesAndWait(new Request(myID, selection, null, RequestType.QUERY)));
            } catch (Exception e) {
                e.printStackTrace();
            }
        }
        else {
            Request queryRequest = new Request(myID, selection, null, RequestType.QUERY_ALL);
            try {
                cursor = cursorFromString(sendToNodeAndWaitForResponse(queryRequest, 0));
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

        private void respond(Cursor cursor) throws IOException {
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
                            respond(cursor);
                            break;
                        case QUERY_ALL:
                            cursor = queryAllLocal();
                            Log.d(TAG, "fetched Cursor " + cursorToString(cursor));
                            respond(cursor);
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
