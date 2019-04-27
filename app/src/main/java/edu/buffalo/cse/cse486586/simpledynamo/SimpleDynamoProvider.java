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
import java.util.Formatter;
import java.util.HashMap;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

public class SimpleDynamoProvider extends ContentProvider {
    static KeyValueStorageDBHelper dbHelper;
    static SQLiteDatabase dbWriter, dbReader;
    static final int selfProcessIdLen = 4;
    static final Integer[] remotePorts = {5554, 5556, 5558, 5560, 5562};
    static Integer myID;
    static String myHash;
    static DynamoCommunicator dynamoCommunicator;

    static final String DELETE_TAG = "DELETE_TAG", CREATE_TAG = "CREATE_TAG",
            INSERT_TAG = "INSERT_TAG", QUERY_TAG = "QUERY_TAG", FETCH_TAG = "FETCH";
    static final String[] projection = new String[]{
            KeyValueStorageContract.KeyValueEntry.COLUMN_KEY,
            KeyValueStorageContract.KeyValueEntry.COLUMN_VALUE
    };

    ExecutorService executorService;
    HashMap<Integer, Client> clientHashMap;

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

    @Override
    public boolean onCreate() {
        /* All initializations */
        dbHelper = new KeyValueStorageDBHelper(getContext());
        dbWriter = dbHelper.getWritableDatabase();
        dbReader = dbHelper.getReadableDatabase();
        myID = getProcessId();

        executorService = Executors.newFixedThreadPool(remotePorts.length);
        clientHashMap = new HashMap<Integer, Client>(remotePorts.length);

        myHash = generateHash(myID.toString());

        Log.d(CREATE_TAG, "id is " + myID + " " + myHash);

        /* Starts the new server thread and wait for it to start*/
        Thread serverThread = new Server();
        serverThread.start();
        try {
            Log.d(CREATE_TAG, "Waiting for server to start");
            synchronized (serverThread) {
                serverThread.wait();
            }
        } catch (InterruptedException e) {
            e.printStackTrace();
        }

        /* Start the clients */
        dynamoCommunicator = new DynamoCommunicator(executorService);

        return true;
    }

    public long insertLocal(ContentValues values) {
        Log.d(INSERT_TAG+ "/ASKED", values.toString());
        return dbWriter.insertWithOnConflict(KeyValueStorageContract.KeyValueEntry.TABLE_NAME,
                null, values, SQLiteDatabase.CONFLICT_REPLACE);
    }

    @Override
    public Uri insert(Uri uri, ContentValues values) {
        Log.d(INSERT_TAG, values.toString());
        String key = values.getAsString("key");
        try {
            dynamoCommunicator.sendToAllReplicas(new Request(myID, values, RequestType.INSERT));
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

        if(cursor.getCount() == 0)
            Log.e(QUERY_TAG, "No value found in table :(" );
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
        Log.d(QUERY_TAG, "Querying " + selection);
        Cursor cursor = null;
        if(selection.equals("@")){
            cursor = queryAllLocal();
        }
        else {
            Request queryRequest = new Request(myID, selection, null, RequestType.QUERY);
            try {
                cursor = cursorFromString(dynamoCommunicator.sendToNodeAndWaitForResponse(queryRequest, 0));
            } catch (IOException e) {
                e.printStackTrace();
            } catch (Exception e) {
                e.printStackTrace();
            }
        }
        if(cursor != null && cursor.getCount() == 0)
            Log.e(QUERY_TAG, "No values found :-(");
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
                while (true) {
                    Request request = null;
                    request = new Request(ois.readUTF());
                    Log.d(TAG, request.toString());
                    switch (request.getRequestType()) {
                        case INSERT:
                            insertLocal(contentValuesFromRequest(request));
                            break;
                        case QUERY:
                            Cursor cursor = queryLocal(request.getHashedKey());
                            Log.d(TAG, "fetched Cursor " + cursorToString(cursor));
                            respond(cursor);
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
