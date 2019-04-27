package edu.buffalo.cse.cse486586.simpledynamo;

import android.util.Log;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Comparator;
import java.util.HashMap;
import java.util.List;
import java.util.TreeMap;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Future;

import static edu.buffalo.cse.cse486586.simpledynamo.SimpleDynamoProvider.generateHash;
import static edu.buffalo.cse.cse486586.simpledynamo.SimpleDynamoProvider.remotePorts;


public final class DynamoCommunicator {
    static ExecutorService executorService;
    static TreeMap<String, List<Integer>> dynamoTreeMap;
    static HashMap<Integer, Client> clientHashMap;
    static final int replicas = 3;
    static final String DYNAMO_TAG = "DYNAMO";

    DynamoCommunicator(ExecutorService executorService, Integer[] remoteIDs) {
        this.executorService = executorService;
        this.dynamoTreeMap = new TreeMap<String, List<Integer>>();
        this.clientHashMap = new HashMap<Integer, Client>(remoteIDs.length);

        /* Sort the array */
        Arrays.sort(remoteIDs, new Comparator<Integer>() {
            @Override
            public int compare(Integer lhs, Integer rhs) {
                return generateHash(lhs.toString()).compareTo(generateHash(rhs.toString()));
            }
        });

        /* Create the TreeMap with all the AVDs to be contacted for a given key */
        /* Also try connecting to the sockets */
        ArrayList<Integer> remoteList;
        int numRemotes = remoteIDs.length;
        for (int i = 0; i < remoteIDs.length; i++) {
            remoteList = new ArrayList<Integer>(replicas);

            Client client = null;
            try {
                client = connectToClient(remotePorts[i]).get();
            } catch (Exception e) {
                Log.e(DYNAMO_TAG, "Unable to connect to remote "+ remotePorts[i]);
            }

            if(client != null)
                Log.d(DYNAMO_TAG, "Connected to remote " + remotePorts[i]);

            clientHashMap.put(remotePorts[i], client);

            /* Add all the AVDs until end of ring */
            for (int j = i; remoteList.size() < replicas; j++) {
                remoteList.add(remoteIDs[j % numRemotes]);
            }
            dynamoTreeMap.put(generateHash(remoteIDs[i].toString()), remoteList);
        }
    }


    /* Sends a given request to the given avdNum in the dynamo ring
     * 0 is the coordinator and 1 onwards are replicas */
    public Future<Void> sendRequest(final Request request, final int remoteNum) {
        return executorService.submit(new Callable<Void>() {
            static final String REQUEST_TAG = "REQUEST_TAG";

            @Override
            public Void call() throws Exception {
                List<Integer> remoteList = null;
                remoteList = dynamoTreeMap.ceilingEntry(request.getHashedKey())
                        .getValue();
                if(remoteList == null){
                    remoteList = dynamoTreeMap.firstEntry().getValue();
                }
                Log.d(REQUEST_TAG, "Remote list is " + remoteList);
                int remoteToContact = remoteList.get(remoteNum);
                if(clientHashMap.get(remoteToContact) == null){
                        clientHashMap.put(remoteToContact, new Client(remoteToContact));
                        Log.d(REQUEST_TAG, "Added new remote " + remoteToContact);
                }
                return null;
            }
        });
    }

    public Future<Client> connectToClient(final int remotePort) {
        return executorService.submit(new Callable<Client>() {
            @Override
            public Client call() throws IOException {
                return new Client(remotePort);
            }
        });
    }
}
