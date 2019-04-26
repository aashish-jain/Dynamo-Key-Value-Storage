package edu.buffalo.cse.cse486586.simpledynamo;

import android.util.Log;

import java.awt.font.TextAttribute;
import java.io.IOException;
import java.sql.Array;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Comparator;
import java.util.List;
import java.util.TreeMap;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Future;

import static edu.buffalo.cse.cse486586.simpledynamo.SimpleDynamoProvider.generateHash;


public final class DynamoCommunicator {
    ExecutorService executorService;
    TreeMap<String, List<Integer>> dynamoTreeMap;
    static final int replicas = 3;

    DynamoCommunicator(ExecutorService executorService, Integer[] avds) {
        this.executorService = executorService;
        this.dynamoTreeMap = new TreeMap<String, List<Integer>>();

        /* Sort the array */
        Arrays.sort(avds, new Comparator<Integer>() {
            @Override
            public int compare(Integer lhs, Integer rhs) {
                return generateHash(lhs.toString()).compareTo(generateHash(rhs.toString()));
            }
        });

        /* Create the TreeMap with all the AVDs to be contacted for a given key */
        ArrayList<Integer> portList;
        for(int i =0; i< avds.length; i++){
            portList = new ArrayList<Integer>(replicas);

            /* Add all the AVDs until end of ring */
            int numAvds = avds.length;
            for(int j = i; portList.size() < replicas; j++){
                portList.add(avds[j % numAvds]);
            }
            dynamoTreeMap.put(generateHash(avds[i].toString()), portList);
        }
    }

    public Future<Client> connectToClient(final int remotePort) {
        return executorService.submit(new Callable<Client>() {
            @Override
            public Client call() throws IOException {
                return new Client(remotePort);
            }
        });
    }

    public Future<Boolean> sendRequest(final Request request) {
        return executorService.submit(new Callable<Boolean>() {
            static final String REQUEST_TAG = "REQUEST_TAG";

            @Override
            public Boolean call() throws Exception {
                return null;
            }
        });
    }
}
