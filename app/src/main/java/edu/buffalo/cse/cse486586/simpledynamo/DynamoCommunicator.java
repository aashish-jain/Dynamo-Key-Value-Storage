package edu.buffalo.cse.cse486586.simpledynamo;

import android.util.Log;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Comparator;
import java.util.HashMap;
import java.util.List;
import java.util.TreeMap;
import java.util.concurrent.ExecutorService;

import static edu.buffalo.cse.cse486586.simpledynamo.SimpleDynamoProvider.generateHash;
import static edu.buffalo.cse.cse486586.simpledynamo.SimpleDynamoProvider.remotePorts;


public final class DynamoCommunicator {
    static TreeMap<String, List<Integer>> dynamoTreeMap;
    static HashMap<Integer, Client> clientHashMap;
    static final int replicas = 3;
    static final String DYNAMO_TAG = "DYNAMO";
    static final String REQUEST_TAG = "REQUEST_TAG";

    DynamoCommunicator(ExecutorService executorService) {
        this.dynamoTreeMap = new TreeMap<String, List<Integer>>();
        this.clientHashMap = new HashMap<Integer, Client>(remotePorts.length);

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
                Log.e(DYNAMO_TAG, "Unable to connect to remote " + SimpleDynamoProvider.remotePorts[i]);
            }

            if (client != null)
                Log.d(DYNAMO_TAG, "Connected to remote " + SimpleDynamoProvider.remotePorts[i]);

            clientHashMap.put(SimpleDynamoProvider.remotePorts[i], client);

            /* Add all the AVDs until end of ring */
            for (int j = i; remoteList.size() < replicas; j++) {
                remoteList.add(remotePorts[j % numRemotes]);
            }

            dynamoTreeMap.put(generateHash(remotePorts[i].toString()), remoteList);
        }
    }

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

    /* Sends a given request to the given avdNum in the dynamo ring
     * 0 is the coordinator and 1 onwards are replicas */
    public synchronized boolean sendToAllReplicas(final Request request) throws Exception {
        List<Integer> remoteList = getRemoteList(request.getHashedKey());
        Log.d(REQUEST_TAG, "Remote list is " + remoteList);

        for (Integer remoteToContact : remoteList) {
            if (clientHashMap.get(remoteToContact) == null) {
                clientHashMap.put(remoteToContact, new Client(remoteToContact));
                Log.d(REQUEST_TAG, "Added new remote " + remoteToContact);
            }
            Client client = clientHashMap.get(remoteToContact);
            client.writeUTF(request.toString());
        }
        return true;
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

}
