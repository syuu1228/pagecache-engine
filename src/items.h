#ifndef ITEMS_H
#define ITEMS_H

/*
 * You should not try to aquire any of the item locks before calling these
 * functions.
 */
typedef struct _hash_item {
    struct _hash_item *next;
    struct _hash_item *prev;
    struct _hash_item *h_next; /* hash chain next */
    rel_time_t time;  /* least recent access */
    rel_time_t exptime; /**< When the item will expire (relative to process
                         * startup) */
    uint32_t nbytes; /**< The total size of the data (in bytes) */
    uint32_t flags; /**< Flags associated with the item (in network byte order)*/
    uint16_t nkey; /**< The total length of the key (in bytes) */
    uint16_t iflag; /**< Intermal flags. lower 8 bit is reserved for the core
                     * server, the upper 8 bits is reserved for engine
                     * implementation. */
    unsigned short refcount;
    void *data;
} hash_item;

typedef struct {
    unsigned int evicted;
    unsigned int evicted_nonzero;
    rel_time_t evicted_time;
    unsigned int outofmemory;
    unsigned int tailrepairs;
    unsigned int reclaimed;
} itemstats_t;

struct items {
    hash_item *heads;
    hash_item *tails;
    itemstats_t itemstats;
    unsigned int sizes;
};


/**
 * Allocate and initialize a new item structure
 * @param engine handle to the storage engine
 * @param key the key for the new item
 * @param nkey the number of bytes in the key
 * @param flags the flags in the new item
 * @param exptime when the object should expire
 * @param nbytes the number of bytes in the body for the item
 * @return a pointer to an item on success NULL otherwise
 */
hash_item *item_alloc(struct pagecache_engine *engine,
                      const void *key, size_t nkey, int flags,
                      rel_time_t exptime, int nbytes, const void *cookie);

/**
 * Get an item from the cache
 *
 * @param engine handle to the storage engine
 * @param key the key for the item to get
 * @param nkey the number of bytes in the key
 * @return pointer to the item if it exists or NULL otherwise
 */
hash_item *item_get(struct pagecache_engine *engine,
                    const void *key, const size_t nkey);

/**
 * Reset the item statistics
 * @param engine handle to the storage engine
 */
void item_stats_reset(struct pagecache_engine *engine);

/**
 * Get detaild item statitistics
 * @param engine handle to the storage engine
 * @param add_stat callback provided by the core used to
 *                 push statistics into the response
 * @param cookie cookie provided by the core to identify the client
 */
void item_stats_sizes(struct pagecache_engine *engine,
                      ADD_STAT add_stat, const void *cookie);

/**
 * Flush expired items from the cache
 * @param engine handle to the storage engine
 * @param when when the items should be flushed
 */
void  item_flush_expired(struct pagecache_engine *engine, time_t when);

/**
 * Release our reference to the current item
 * @param engine handle to the storage engine
 * @param it the item to release
 */
void item_release(struct pagecache_engine *engine, hash_item *it);

/**
 * Unlink the item from the hash table (make it inaccessible)
 * @param engine handle to the storage engine
 * @param it the item to unlink
 */
void item_unlink(struct pagecache_engine *engine, hash_item *it);

/**
 * Store an item in the cache
 * @param engine handle to the storage engine
 * @param item the item to store
 * @param operation what kind of store operation is this (ADD/SET etc)
 * @return ENGINE_SUCCESS on success
 */
ENGINE_ERROR_CODE store_item(struct pagecache_engine *engine,
                             hash_item *item,
                             ENGINE_STORE_OPERATION operation,
                             const void *cookie);
#endif
