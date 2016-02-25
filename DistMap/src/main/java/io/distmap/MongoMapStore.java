package io.distmap;

import com.hazelcast.core.MapStore;
import com.mongodb.MongoClient;
import com.mongodb.MongoCredential;
import com.mongodb.ServerAddress;
import org.mongodb.morphia.Morphia;

import java.util.*;
import java.util.stream.Collectors;

/**
 * Created by אלכס on 25/02/2016.
 */
public class MongoMapStore<T> implements MapStore<T, T> {

    public void connectToDB(DBInfo dbInfo){
        MongoClient client = createClient(dbInfo);
    }

    private MongoClient createClient(DBInfo dbInfo) {
        MongoCredential cred = MongoCredential.createScramSha1Credential(dbInfo.getUserName(), dbInfo.getDbName(), dbInfo.getPassword().toCharArray());
        List<MongoCredential> creds = new LinkedList<>();
        creds.add(cred);
        return new MongoClient(parseHosts(dbInfo.getDbHosts()), creds);
    }

    private List<ServerAddress> parseHosts(List<String> dbHosts) {
        dbHosts.stream().map(x -> {
            String[] ipAndHost = x.split(":");
            if(ipAndHost.length>1){
                return new ServerAddress(ipAndHost[0], Integer.parseInt(ipAndHost[1]));
            }else{
                return new ServerAddress(ipAndHost[0]);
            }
        }).collect(Collectors.toList());
        return null;
    }

    @Override
    public void store(T t, T t2) {

    }

    @Override
    public void storeAll(Map<T, T> map) {

    }

    @Override
    public void delete(T t) {

    }

    @Override
    public void deleteAll(Collection<T> collection) {

    }

    @Override
    public T load(T t) {
        return null;
    }

    @Override
    public Map<T, T> loadAll(Collection<T> collection) {
        return null;
    }

    @Override
    public Iterable<T> loadAllKeys() {
        return null;
    }
}
