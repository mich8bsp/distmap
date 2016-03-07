package io.distmap.redis;

import java.util.Map;

/**
 * Created by mich8bsp on 07-Mar-16.
 */
public class NotificationMsg<K,V> {
    private SimpleMapEntry<K,V> update;
    private UpdateStatus status;

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;

        NotificationMsg that = (NotificationMsg) o;

        if (update != null ? !update.equals(that.update) : that.update != null) return false;
        return status == that.status;

    }

    @Override
    public int hashCode() {
        int result = update != null ? update.hashCode() : 0;
        result = 31 * result + (status != null ? status.hashCode() : 0);
        return result;
    }

    public SimpleMapEntry<K,V> getUpdate() {

        return update;
    }

    public void setUpdate(SimpleMapEntry<K,V> update) {
        this.update = update;
    }

    public UpdateStatus getStatus() {
        return status;
    }

    public void setStatus(UpdateStatus status) {
        this.status = status;
    }
}
