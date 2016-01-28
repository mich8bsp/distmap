package io.distmap;

import com.hazelcast.core.EntryEvent;
import com.hazelcast.map.listener.EntryAddedListener;
import com.hazelcast.map.listener.EntryRemovedListener;
import com.hazelcast.map.listener.EntryUpdatedListener;

/**
 * Created by mich8bsp on 28-Jan-16.
 */
public class MapCallback<K, V> implements EntryAddedListener<K,V>, EntryUpdatedListener<K,V>, EntryRemovedListener<K,V> {

    public void entryAdded(EntryEvent<K, V> event) {

    }

    public void entryRemoved(EntryEvent<K, V> event) {

    }

    public void entryUpdated(EntryEvent<K, V> event) {

    }
}
