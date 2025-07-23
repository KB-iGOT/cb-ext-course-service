package com.igot.cb.model;

public class CachedIdMap {
    private long value;
    private long cachedTimeMillis;

    public CachedIdMap(long value, long cachedTimeMillis) {
        this.value = value;
        this.cachedTimeMillis = cachedTimeMillis;
    }

    public boolean isExpired(long ttlMillis) {
        return (System.currentTimeMillis() - cachedTimeMillis) > ttlMillis;
    }

    public long getValue() {
        return value;
    }
}
