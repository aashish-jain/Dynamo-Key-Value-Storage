package edu.buffalo.cse.cse486586.simpledynamo;

import android.content.ContentValues;
import android.util.Log;

import java.io.IOException;

import static edu.buffalo.cse.cse486586.simpledynamo.Utils.generateHash;

enum RequestType {
    QUERY, QUERY_ALL, INSERT, DELETE, DELETE_ALL, FETCH_FAILED;
}

public class Request {
    static final String TAG = "REQUEST";
    private static final String seperator = ",";

    private int senderId;
    private String hashedKey;
    private RequestType requestType;
    private String key, value;

    Request(int senderId, String key, String value, RequestType requestType) {
        this.senderId = senderId;
        this.requestType = requestType;
        this.key = key;
        if (key != null)
            this.hashedKey = generateHash(key);
        this.value = value;
    }

    /* To parse from the string */
    Request(String string) throws IOException {
        String[] strings = string.split(this.seperator);
        if (strings.length == 5) {
            this.requestType = RequestType.valueOf(strings[0]);
            this.senderId = Integer.parseInt(strings[1]);
            this.hashedKey = strings[2];
            this.key = strings[3];
            this.value = strings[4];
        } else {
            Log.d(TAG, string + " " + strings.length);
            throw new IOException("Unable to parse the String");
        }
    }

    Request(int senderId, ContentValues contentValues, RequestType requestType) {
        this.requestType = requestType;
        this.senderId = senderId;
        this.key = contentValues.getAsString("key");
        this.value = contentValues.getAsString("value");
        if (this.key != null) {
            this.hashedKey = generateHash(this.key);
        }
    }

    public RequestType getRequestType() {
        return requestType;
    }

    public String getKey() {
        return key;
    }

    public String getValue() {
        return value;
    }

    public String getHashedKey() {
        return this.hashedKey;
    }

    @Override
    public String toString() {
        return requestType + seperator + senderId + seperator + hashedKey + seperator +
                key + seperator + value;
    }

    public int getSender(){
        return senderId;
    }
}
