package edu.buffalo.cse.cse486586.simpledynamo;

import android.content.ContentProvider;
import android.content.ContentValues;
import android.content.Context;
import android.database.Cursor;
import android.database.sqlite.SQLiteDatabase;
import android.net.Uri;
import android.os.StrictMode;
import android.telephony.TelephonyManager;
import android.util.Log;

import java.io.IOException;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.net.InetSocketAddress;
import java.net.ServerSocket;
import java.net.Socket;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.Comparator;
import java.util.HashMap;
import java.util.List;
import java.util.TreeMap;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.atomic.AtomicBoolean;

import static edu.buffalo.cse.cse486586.simpledynamo.Utils.contentValuesFromRequest;
import static edu.buffalo.cse.cse486586.simpledynamo.Utils.cursorFromString;
import static edu.buffalo.cse.cse486586.simpledynamo.Utils.cursorToString;
import static edu.buffalo.cse.cse486586.simpledynamo.Utils.generateHash;

public class SimpleDynamoProvider extends ContentProvider {
    private static final Integer[] remotePorts = {5554, 5556, 5558, 5560, 5562};
    private static final List remotePortList = new ArrayList(Arrays.asList(remotePorts));
    private static final int selfProcessIdLen = 4, replicas = 3;
    private static final String[] projection = new String[]{
            KeyValueStorageContract.KeyValueEntry.COLUMN_KEY,
            KeyValueStorageContract.KeyValueEntry.COLUMN_VALUE
    };

    private enum SendType {
        REPLICAS, ALL
    }

    private AtomicBoolean recoveryStatus = new AtomicBoolean(true);

    private static final String DELETE = "DELETE", DELETED = "DELETED", CREATE = "CREATE",
            INSERT = "INSERT", INSERTED = "INSERTED", QUERY = "QUERY",
            QUERIED = "QUERIED", SEND = "SEND", FAILURES = "FAILURES";

    private KeyValueStorageDBHelper dbHelper;
    private SQLiteDatabase dbWriter, dbReader;
    private Integer myID;
    private TreeMap<String, List<Integer>> replicaMap;
    private HashMap<Integer, ConcurrentLinkedQueue<Request>> failedRequests;

    private static void enableStrictMode() {
        StrictMode.ThreadPolicy policy = new StrictMode.ThreadPolicy.Builder().permitAll().build();
        StrictMode.setThreadPolicy(policy);
    }

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
        List<Integer> remoteList;
        try {
            remoteList = replicaMap.ceilingEntry(key)
                    .getValue();
        } catch (NullPointerException e) {
            remoteList = replicaMap.firstEntry().getValue();
        }
        return remoteList;
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

            /* Add all the AVDs until end of ring */
            for (int j = i; remoteList.size() < replicas; j++)
                remoteList.add(remotePorts[j % numRemotes]);

            replicaMap.put(generateHash(remotePorts[i].toString()), remoteList);
        }
    }

    private void fetchFailures() {
        Request fetchRequest = new Request(myID, null, null, RequestType.FETCH_FAILED);
        String response = send(fetchRequest, SendType.ALL, true);
        if (response == null || response.equals(""))
            return;

        List<String> operations = Arrays.asList(response.split("\n"));

        List<Request> requestList = new ArrayList<Request>(operations.size());
        for (String operation : operations) {
            try {
                requestList.add(new Request(operation));
            } catch (IOException e) {
                Log.e(CREATE, "Error in fetching messages. Possibly no Failure occurred");
            }
        }

        Collections.sort(requestList, new Comparator<Request>() {
            @Override
            public int compare(Request lhs, Request rhs) {
                return new Long(lhs.getTime() - rhs.getTime()).intValue();
            }
        });

        Log.d(FAILURES, requestList.toString());

        for (Request request : requestList) {
            Log.d(FAILURES, request.toString());
            switch (request.getRequestType()) {
                case INSERT:
                    insertLocal(contentValuesFromRequest(request));
                    break;
                case QUERY:
                    queryLocal(request.getKey());
                    break;
                case DELETE:
                    deleteLocal(request.getKey());
                    break;
            }
        }
    }


    private synchronized String send(Request request, SendType type, boolean get) {
        List<Integer> to_send;
        switch (type) {
            case REPLICAS:
                to_send = getReplicaList(request.getHashedKey());
                break;
            case ALL:
                to_send = remotePortList;
                break;
            default:
                Log.e(SEND, "Invalid Send Type");
                return null;
        }

        StringBuilder stringBuilder = new StringBuilder();
        Request quitRequest = new Request(myID, null, RequestType.QUIT);
        Log.d(SEND, "NODES " + to_send.toString());
        for (Integer remote : to_send) {
            try {
                Client client = new Client(remote);
                client.writeUTF(request.toString());
                Log.d(SEND, request.toString() + " to " + remote);
                if (get) {
                    String response = client.readUTF();
                    if (!response.equals("done") && !response.equals(" ")) {
                        stringBuilder.append(response);
                    } else if (response.equals("done")) {
                    }
                }
                client.writeUTF(quitRequest.toString());
            } catch (Exception e) {
                /* Hack for missed messages */
                RequestType requestType = request.getRequestType();
                if (requestType == RequestType.INSERT || requestType == RequestType.DELETE) {
                    failedRequests.get(remote).offer(request);
                    Log.e(SEND, "Possible Failure at node " + remote + ". Count = " + failedRequests.get(remote).size());
                } else {
                    Log.e(SEND, "Unable to send message to " + remote);
                }
            }
        }

        if (!get)
            return null;
        return stringBuilder.toString();
    }

    @Override
    public boolean onCreate() {
        /* All initializations */
        enableStrictMode();

        dbHelper = new KeyValueStorageDBHelper(getContext());
        dbWriter = dbHelper.getWritableDatabase();
        dbReader = dbHelper.getReadableDatabase();
        myID = getProcessId();

        this.replicaMap = new TreeMap<String, List<Integer>>();

        /* Starts the new server thread and wait for it to start*/
        Thread serverThread = new Server();
        serverThread.start();
        Log.d(CREATE, "Waiting for server to start");

        /* Start the clients */
        initializeDynamoTreeMap();

        failedRequests = new HashMap<Integer, ConcurrentLinkedQueue<Request>>();
        /* Connect to clients if they are up and also initialize LinkedBlockingQueue */
        for (Integer remotePort : remotePorts)
            failedRequests.put(remotePort, new ConcurrentLinkedQueue<Request>());
        Log.d(CREATE, "id is " + myID + " " + generateHash(myID.toString()));


        /* Fetch Failures*/
        fetchFailures();
        recoveryStatus.set(false);
        return true;
    }

    private long insertLocal(ContentValues values) {
        dbWriter.insertWithOnConflict(KeyValueStorageContract.KeyValueEntry.TABLE_NAME,
                null, values, SQLiteDatabase.CONFLICT_REPLACE);
        int rows = dbReader.query(KeyValueStorageContract.KeyValueEntry.TABLE_NAME, this.projection,
                null, null, null, null,
                null, null).getCount();
        Log.d(INSERTED, values.toString() + "Now have " + rows);
        return 0;
    }

    @Override
    public Uri insert(Uri uri, ContentValues values) {
        Log.d(INSERT, values.toString());
        Request insertRequest = new Request(myID, values, RequestType.INSERT);
        send(insertRequest, SendType.REPLICAS, false);
        return uri;
    }

    @Override
    public int update(Uri uri, ContentValues values, String selection,
                      String[] selectionArgs) {
        return 0;
    }

    private Cursor queryLocal(String key) {
        Log.d(QUERIED, "Querying " + key);
        String[] selectionArgs = new String[]{key};
        String selection = KeyValueStorageContract.KeyValueEntry.COLUMN_KEY + " = ?";

        /* https://developer.android.com/training/data-storage/sqlite */
        Cursor cursor = dbReader.query(KeyValueStorageContract.KeyValueEntry.TABLE_NAME, this.projection,
                selection, selectionArgs, null, null, null);
        return cursor;
    }

    private Cursor queryAllLocal() {
        // Query everything
        return dbReader.query(KeyValueStorageContract.KeyValueEntry.TABLE_NAME, this.projection,
                null, null, null, null,
                null, null);
    }


    @Override
    public Cursor query(Uri uri, String[] projection, String selection,
                        String[] selectionArgs, String sortOrder) {
        Log.d(QUERY, "Querying " + selection);
        Cursor cursor;

        if (selection.equals("@")) {
            cursor = queryAllLocal();
        } else if (selection.equals("*")) {
            Request queryRequest = new Request(myID, selection, null, RequestType.QUERY_ALL);
            cursor = cursorFromString(send(queryRequest, SendType.ALL, true));
            Log.d(QUERY, cursorToString(cursor));
        } else {
            Request queryRequest = new Request(myID, selection, null, RequestType.QUERY);
            /* TODO: Fix some missing messages from avds */
            cursor = cursorFromString(send(queryRequest, SendType.ALL, true));
        }
        if (cursor != null && cursor.getCount() == 0)
            Log.e(QUERY, "No values found :-(");
        return cursor;
    }

    private int deleteLocal(String key) {
        Log.d(DELETED, key);
        //https://stackoverflow.com/questions/7510219/deleting-row-in-sqlite-in-android
        return dbWriter.delete(KeyValueStorageContract.KeyValueEntry.TABLE_NAME,
                KeyValueStorageContract.KeyValueEntry.COLUMN_KEY + "='" + key + "'",
                null);
    }

    private int deleteAllLocal() {
        return dbWriter.delete(KeyValueStorageContract.KeyValueEntry.TABLE_NAME,
                null, null);
    }

    @Override
    public int delete(Uri uri, String selection, String[] selectionArgs) {
        Log.d(DELETE, selection);
        if (selection.equals("@")) {
            deleteAllLocal();
        } else if (selection.equals("*")) {
            Request deleteRequest = new Request(myID, selection, null, RequestType.DELETE_ALL);
            send(deleteRequest, SendType.ALL, false);
        } else {
            Request deleteRequest = new Request(myID, selection, null, RequestType.DELETE);
            send(deleteRequest, SendType.REPLICAS, false);
        }
        return 0;
    }


    private class Server extends Thread {
        static final int SERVER_PORT = 10000;
        static final String TAG = "SERVER_TASK";

        /* https://stackoverflow.com/questions/10131377/socket-programming-multiple-client-to-one-server*/
        @Override
        public void run() {
            /* Open a socket at SERVER_PORT */
            ServerSocket serverSocket = null;
            try {
                /* https://stackoverflow.com/questions/24615704/socket-eaddrinuse-address-already-in-use */
                /* Fix for EADDRINUSE */
                serverSocket = new ServerSocket();
                serverSocket.setReuseAddress(true);
                serverSocket.bind(new InetSocketAddress(SERVER_PORT));
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
                    break;
                }
            }
            try {
                serverSocket.close();
            } catch (IOException e) {
                e.printStackTrace();
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

        private void sendResponse(Cursor cursor) throws IOException {
            oos.writeUTF(cursorToString(cursor));
            oos.flush();
        }

        private void sendFailed(int requesterId) throws IOException {
            ConcurrentLinkedQueue<Request> queue = failedRequests.get(requesterId);
            StringBuilder stringBuilder = new StringBuilder();
            Request request;
            while (queue.size() > 0) {
                request = queue.poll();
                stringBuilder.append(request.toString());
                stringBuilder.append("\n");
            }
            Log.e(FAILURES, "\n" + stringBuilder.toString());
            oos.writeUTF(stringBuilder.toString());
            oos.flush();
        }

        @Override
        public void run() {
            Cursor cursor;
            Request request;
            try {
                while (true) {
                    /* Hack for Waiting for the recovery to complete */
                    while (recoveryStatus.get()){
                        try {
                            Thread.sleep(100);
                        } catch (InterruptedException e) {
                            e.printStackTrace();
                        }
                    }
                    request = new Request(ois.readUTF());
                    Log.d(TAG, request.toString());
                    switch (request.getRequestType()) {
                        case INSERT:
                            insertLocal(contentValuesFromRequest(request));
                            break;
                        case QUERY:
                            cursor = queryLocal(request.getKey());
                            sendResponse(cursor);
                            break;
                        case QUERY_ALL:
                            cursor = queryAllLocal();
                            sendResponse(cursor);
                            break;
                        case DELETE:
                            deleteLocal(request.getKey());
                            break;
                        case DELETE_ALL:
                            deleteAllLocal();
                            break;
                        case FETCH_FAILED:
                            sendFailed(request.getSender());
                            break;
                        case QUIT:
                            return;
                        default:
                            Log.d(TAG, "Unknown Operation. :-?");
                            return;
                    }
                }
            } catch (IOException e) {
                Log.e(TAG, "Possible Client Failure");
            }
        }
    }
}