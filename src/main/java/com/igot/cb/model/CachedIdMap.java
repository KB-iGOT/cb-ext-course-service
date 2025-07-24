package com.igot.cb.model;

/**
 * CachedIdMap is a simple class that holds an Integer value and the time it was cached.
 * It provides a method to check if the cached value has expired based on a given TTL (time-to-live).
 */
public class CachedIdMap {
    private Integer value;
    private long cachedTimeMillis;

    /**
     * Constructor to create a CachedIdMap with the current time as the cached time.
     *
     * @param value The Integer value to be cached.
     */
    public CachedIdMap(Integer value, long cachedTimeMillis) {
        this.value = value;
        this.cachedTimeMillis = cachedTimeMillis;
    }

    /**
     * Checks if the cached value has expired based on the provided TTL in milliseconds.
     *
     * @param ttlMillis The time-to-live in milliseconds.
     * @return true if the cached value is expired, false otherwise.
     */
    public boolean isExpired(long ttlMillis) {
        return (System.currentTimeMillis() - cachedTimeMillis) > ttlMillis;
    }

    /**
     * Gets the cached Integer value.
     *
     * @return The cached Integer value.
     */
    public Integer getValue() {
        return value;
    }
}
