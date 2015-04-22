package org.apache.spark.util.collection;

import java.util.LinkedHashMap;
import java.util.Map;

/**
 * Created by haow1990 on 4/22/15.
 */
public abstract class HaoLruCache extends LinkedHashMap<Long, Object>{
    public HaoLruCache(long maxBytes) {
        this.maxBytes = maxBytes;
    }
    private Long maxBytes = 0L;
    private Long curBytes = 0L;

    @Override
    public Object remove(Object key) {
        Object val = super.remove(key);
        curBytes -= getSize(val);
        return val;
    }

    @Override
    public Object put(Long key, Object value) {
        curBytes += getSize(value);
        return super.put(key, value);
    }

    @Override
    protected boolean removeEldestEntry(final Map.Entry<Long, Object> eldest) {
        if (maxBytes > 0 && curBytes > maxBytes) {
            return true;
        } else {
            return false;
        }
    }

    protected abstract long getSize(Object value);
}
