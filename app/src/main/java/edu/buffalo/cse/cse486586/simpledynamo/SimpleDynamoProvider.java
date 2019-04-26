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

import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.util.Formatter;
import java.util.HashMap;
import java.util.concurrent.ExecutionException;
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

        /* Starts the new server thread */
        new Server().start();

        dynamoCommunicator = new DynamoCommunicator(executorService, remotePorts);

        /* Attempts to all client sockets with respective servers */
        for (int remotePort : remotePorts) {
            Client clientObject = null;
            try {
                clientObject = dynamoCommunicator.connectToClient(remotePort).get();
            } catch (Exception e) {
                Log.e(CREATE_TAG, "Unable to connect to port " + remotePort);
            } finally {
                clientHashMap.put(remotePort, clientObject);
            }
        }
        return true;
    }

    public long insertLocal(ContentValues values) {
        return dbWriter.insertWithOnConflict(KeyValueStorageContract.KeyValueEntry.TABLE_NAME,
                null, values, SQLiteDatabase.CONFLICT_REPLACE);
    }

    @Override
    public Uri insert(Uri uri, ContentValues values) {
        Log.d(INSERT_TAG, values.toString());
        String key = values.getAsString("key");
        try {
            dynamoCommunicator.sendRequest(new Request(myID, values, RequestType.INSERT)).get();
        } catch (InterruptedException e) {
            e.printStackTrace();
        } catch (ExecutionException e) {
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

    @Override
    public Cursor query(Uri uri, String[] projection, String selection,
                        String[] selectionArgs, String sortOrder) {
        // TODO Auto-generated method stub
        return null;
    }


    @Override
    public int delete(Uri uri, String selection, String[] selectionArgs) {
        // TODO Auto-generated method stub
        return 0;
    }

}
