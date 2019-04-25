package edu.buffalo.cse.cse486586.simpledynamo;

import android.content.ContentProvider;
import android.content.ContentValues;
import android.content.Context;
import android.database.Cursor;
import android.database.MatrixCursor;
import android.database.sqlite.SQLiteDatabase;
import android.net.Uri;
import android.os.StrictMode;
import android.telephony.TelephonyManager;
import android.util.Log;

import java.io.IOException;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.util.Comparator;
import java.util.Formatter;
import java.util.HashMap;
import java.util.TreeMap;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;

public class SimpleDynamoProvider extends ContentProvider {
    static KeyValueStorageDBHelper dbHelper;
    static SQLiteDatabase dbWriter, dbReader;
    static final int selfProcessIdLen = 4;
    static final int[] remotePorts = {5554, 5556, 5558, 5560, 5562};
    static Integer myID;
    static String myHash;

    static final String DELETE_TAG = "DELETE_TAG", CREATE_TAG = "CREATE_TAG",
            INSERT_TAG = "INSERT_TAG", QUERY_TAG = "QUERY_TAG", FETCH_TAG = "FETCH";
    static final String[] projection = new String[]{
            KeyValueStorageContract.KeyValueEntry.COLUMN_KEY,
            KeyValueStorageContract.KeyValueEntry.COLUMN_VALUE
    };

    ExecutorService executorService;
    TreeMap<Integer, Client> clientTreeMap;

    @Override
    public String getType(Uri uri) {
        return "string";
    }

    public static void enableStrictMode() {
        StrictMode.ThreadPolicy policy = new StrictMode.ThreadPolicy.Builder().permitAll().build();
        StrictMode.setThreadPolicy(policy);
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

    public Future<Client> connectToClient(final int remotePort) {
        return executorService.submit(new Callable<Client>() {
            @Override
            public Client call() throws IOException {
                return new Client(remotePort);
            }
        });
    }

    @Override
    public boolean onCreate() {
        /* All initializations */
        dbHelper = new KeyValueStorageDBHelper(getContext());
        dbWriter = dbHelper.getWritableDatabase();
        dbReader = dbHelper.getReadableDatabase();
        myID = getProcessId();

        executorService = Executors.newFixedThreadPool(remotePorts.length);
        clientTreeMap = new TreeMap<Integer, Client>(new Comparator<Integer>() {
            @Override
            public int compare(Integer lhs, Integer rhs) {
                String lhsHash = generateHash(lhs.toString()),
                        rhsHash = generateHash(rhs.toString());
                int result = lhs.compareTo(rhs);
                if(result > 0)
                    return  1;
                else if(result < 0)
                    return  -1;
                else
                    return 0;
            }
        });

        myHash = generateHash(myID.toString());

        Log.d(CREATE_TAG, "id is " + myID + " " + myHash);

        /* Starts the new server thread */
        new Server().start();

        enableStrictMode();

        /* Small delay to ensure that the server of the node starts
         * This is to ensure that the AVDs server is up and
         * can be connected to
         * */
        try {
            Thread.sleep(1000);
        } catch (InterruptedException e) {
            e.printStackTrace();
        }

        /* Attempts to all client sockets with respective servers */
        for (int remotePort : remotePorts)
            clientTreeMap.put(remotePort, new Client(remotePort));
        
        return true;
    }

    public long insertLocal(ContentValues values) {
        return dbWriter.insertWithOnConflict(KeyValueStorageContract.KeyValueEntry.TABLE_NAME,
                null, values, SQLiteDatabase.CONFLICT_REPLACE);
    }

    private Boolean belongsToMe(String key) {
        String keyHash = generateHash(key);
        // If has neighbours
        Boolean toReturn = null;
        return toReturn;
    }

    public void insertInDHT(ContentValues values) throws IOException {
        String key = values.getAsString("key"), value = values.getAsString("value");
//        successor.oos.writeUTF(new Request(myID, key, value, RequestType.INSERT).toString());
//        successor.oos.flush();
    }

    @Override
    public Uri insert(Uri uri, ContentValues values) {
        Log.d(INSERT_TAG, values.toString());
        String key = values.getAsString("key");
        if (belongsToMe(key))
            insertLocal(values);
        else
            try {
                insertInDHT(values);
            } catch (IOException e) {
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
