package edu.buffalo.cse.cse486586.simpledynamo;

import android.content.ContentValues;
import android.util.Log;

import java.io.IOException;

import static edu.buffalo.cse.cse486586.simpledynamo.Utils.generateHash;

enum RequestType {
    QUERY, QUERY_ALL, INSERT, DELETE, DELETE_ALL, FETCH_FAILED, QUIT
}

public class Request {
    static final String TAG = "REQUEST";
    private static final String separator = ",";
    private long time;
    private int senderId;
    private String hashedKey;
    private RequestType requestType;
    private String key, value;

    Request(int senderId, String key, String value, RequestType requestType) {
        this.time = System.currentTimeMillis();
        this.senderId = senderId;
        this.requestType = requestType;
        this.key = key;
        if (key != null)
            this.hashedKey = generateHash(key);
        this.value = value;
    }

    /* To parse from the string */
    Request(String string) throws IOException {
        this.time =System.currentTimeMillis();
        String[] strings = string.split(this.separator);
        if (strings.length == 6) {
            this.requestType = RequestType.valueOf(strings[0]);
            this.senderId = Integer.parseInt(strings[1]);
            this.hashedKey = strings[2];
            this.key = strings[3];
            this.value = strings[4];
            this.time = Long.parseLong(strings[5]);
        } else {
            Log.d(TAG, string + " " + strings.length);
            throw new IOException("Unable to parse the String");
        }
    }

    Request(int senderId, ContentValues contentValues, RequestType requestType) {
        this.time = System.currentTimeMillis();
        this.requestType = requestType;
        this.senderId = senderId;
        if(contentValues!=null) {
            this.key = contentValues.getAsString("key");
            this.value = contentValues.getAsString("value");
        }
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

    public long getTime(){
        return  this.time;
    }

    @Override
    public String toString() {
        return requestType + separator + senderId + separator + hashedKey + separator +
                key + separator + value + separator + time;
    }

    public int getSender() {
        return senderId;
    }
}
