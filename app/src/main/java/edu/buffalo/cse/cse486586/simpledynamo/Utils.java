package edu.buffalo.cse.cse486586.simpledynamo;

import android.content.ContentValues;
import android.database.Cursor;
import android.database.MatrixCursor;
import android.util.Log;

import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.util.Formatter;

public class Utils {
    /* https://stackoverflow.com/questions/3105080/output-values-found-in-cursor-to-logcat-android */
    public static String cursorToString(Cursor cursor) {
        StringBuilder stringBuilder = new StringBuilder();
        if (cursor.moveToFirst()) {
            do {
                int columnsQty = cursor.getColumnCount();
                for (int idx = 0; idx < columnsQty; ++idx) {
                    stringBuilder.append(cursor.getString(idx));
                    if (idx < columnsQty - 1)
                        stringBuilder.append(",");
                }
                stringBuilder.append("\n");
            } while (cursor.moveToNext());
        }
        return stringBuilder.toString();
    }

    public static Cursor cursorFromString(String queryResult) {
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

    public static ContentValues contentValuesFromRequest(Request request) {
        ContentValues contentValues = new ContentValues();
        contentValues.put("key", request.getKey());
        contentValues.put("value", request.getValue());
        return contentValues;
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


}

