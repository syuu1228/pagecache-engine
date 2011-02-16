/* -*- Mode: C; tab-width: 4; c-basic-offset: 4; indent-tabs-mode: nil -*- */

#include <sys/stat.h>
#include <sys/socket.h>
#include <sys/signal.h>
#include <sys/resource.h>
#include <sys/mman.h>
#include <netinet/in.h>

#include <fcntl.h>
#include <errno.h>
#include <stdlib.h>
#include <stdio.h>
#include <string.h>
#include <time.h>
#include <assert.h>
#include <inttypes.h>

#include "pagecache_engine.h"

/* Forward Declarations */
static void item_link_q(struct pagecache_engine *engine, hash_item *it);
static void item_unlink_q(struct pagecache_engine *engine, hash_item *it);
static hash_item *do_item_alloc(struct pagecache_engine *engine,
                                const void *key, const size_t nkey,
                                const int flags, const rel_time_t exptime,
                                const int nbytes,
                                const void *cookie);
static hash_item *do_item_get(struct pagecache_engine *engine,
                              const char *key, const size_t nkey);
static int do_item_link(struct pagecache_engine *engine, hash_item *it);
static void do_item_unlink(struct pagecache_engine *engine, hash_item *it);
static void do_item_release(struct pagecache_engine *engine, hash_item *it);
static void do_item_update(struct pagecache_engine *engine, hash_item *it);
static int do_item_replace(struct pagecache_engine *engine,
                            hash_item *it, hash_item *new_it);
static void item_free(struct pagecache_engine *engine, hash_item *it);


/*
 * We only reposition items in the LRU queue if they haven't been repositioned
 * in this many seconds. That saves us from churning on frequently-accessed
 * items.
 */
#define ITEM_UPDATE_INTERVAL 60

void item_stats_reset(struct pagecache_engine *engine) {
    pthread_mutex_lock(&engine->cache_lock);
    memset(&engine->items.itemstats, 0, sizeof(engine->items.itemstats));
    pthread_mutex_unlock(&engine->cache_lock);
}


/* warning: don't use these macros with a function, as it evals its arg twice */
static inline size_t ITEM_ntotal(struct pagecache_engine *engine,
                                 const hash_item *it) {
    size_t ret = sizeof(*it) + it->nkey + it->nbytes;
    if (engine->config.use_cas) {
        ret += sizeof(uint64_t);
    }

    return ret;
}

/* Get the next CAS id for a new item. */
static uint64_t get_cas_id(void) {
    static uint64_t cas_id = 0;
    return ++cas_id;
}

/*@null@*/
hash_item *do_item_alloc(struct pagecache_engine *engine,
                         const void *key,
                         const size_t nkey,
                         const int flags,
                         const rel_time_t exptime,
                         const int nbytes,
                         const void *cookie) {
    hash_item *it = NULL;
    size_t ntotal = sizeof(hash_item) + nkey;

    rel_time_t current_time = engine->server.core->get_current_time();

    if ((it = (hash_item *)malloc(ntotal)) == NULL) {
        return NULL;
    }

    it->data = mmap(NULL, nbytes, PROT_READ | PROT_WRITE, MAP_PRIVATE, -1, 0);
    if(it->data == MAP_FAILED) {
        free(it);
        return NULL;
    }

    assert(it != engine->items.heads);

    it->next = it->prev = it->h_next = 0;
    it->refcount = 1;     /* the caller will have a reference */
    it->iflag = 0;
    it->nkey = nkey;
    it->nbytes = nbytes;
    it->flags = flags;
    memcpy((void*)item_get_key(it), key, nkey);
    it->exptime = exptime;
    return it;
}

static void item_free(struct pagecache_engine *engine, hash_item *it) {
    size_t ntotal = ITEM_ntotal(engine, it);
    unsigned int clsid;
    assert((it->iflag & ITEM_LINKED) == 0);
    assert(it != engine->items.heads);
    assert(it != engine->items.tails);
    assert(it->refcount == 0);

    /* so slab size changer can tell later if item is already free or not */
    it->iflag |= ITEM_SLABBED;
    munmap(it->data, it->nbytes);
    free(it);
}

static void item_link_q(struct pagecache_engine *engine, hash_item *it) { /* item is the new head */
    hash_item **head, **tail;
    assert((it->iflag & ITEM_SLABBED) == 0);

    head = &engine->items.heads;
    tail = &engine->items.tails;
    assert(it != *head);
    assert((*head && *tail) || (*head == 0 && *tail == 0));
    it->prev = 0;
    it->next = *head;
    if (it->next) it->next->prev = it;
    *head = it;
    if (*tail == 0) *tail = it;
    engine->items.sizes++;
    return;
}

static void item_unlink_q(struct pagecache_engine *engine, hash_item *it) {
    hash_item **head, **tail;
    head = &engine->items.heads;
    tail = &engine->items.tails;

    if (*head == it) {
        assert(it->prev == 0);
        *head = it->next;
    }
    if (*tail == it) {
        assert(it->next == 0);
        *tail = it->prev;
    }
    assert(it->next != it);
    assert(it->prev != it);

    if (it->next) it->next->prev = it->prev;
    if (it->prev) it->prev->next = it->next;
    engine->items.sizes--;
    return;
}

int do_item_link(struct pagecache_engine *engine, hash_item *it) {
    assert((it->iflag & (ITEM_LINKED|ITEM_SLABBED)) == 0);
    assert(it->nbytes < (1024 * 1024));  /* 1MB max size */
    it->iflag |= ITEM_LINKED;
    it->time = engine->server.core->get_current_time();
    assoc_insert(engine, engine->server.core->hash(item_get_key(it), it->nkey, 0),
                 it);

    pthread_mutex_lock(&engine->stats.lock);
    engine->stats.curr_bytes += ITEM_ntotal(engine, it);
    engine->stats.curr_items += 1;
    engine->stats.total_items += 1;
    pthread_mutex_unlock(&engine->stats.lock);

    item_link_q(engine, it);

    return 1;
}

void do_item_unlink(struct pagecache_engine *engine, hash_item *it) {
    if ((it->iflag & ITEM_LINKED) != 0) {
        it->iflag &= ~ITEM_LINKED;
        pthread_mutex_lock(&engine->stats.lock);
        engine->stats.curr_bytes -= ITEM_ntotal(engine, it);
        engine->stats.curr_items -= 1;
        pthread_mutex_unlock(&engine->stats.lock);
        assoc_delete(engine, engine->server.core->hash(item_get_key(it), it->nkey, 0),
                     item_get_key(it), it->nkey);
        item_unlink_q(engine, it);
        if (it->refcount == 0) {
            item_free(engine, it);
        }
    }
}

void do_item_release(struct pagecache_engine *engine, hash_item *it) {
    if (it->refcount != 0) {
        it->refcount--;
    }
    if (it->refcount == 0 && (it->iflag & ITEM_LINKED) == 0) {
        item_free(engine, it);
    }
}

void do_item_update(struct pagecache_engine *engine, hash_item *it) {
    rel_time_t current_time = engine->server.core->get_current_time();
    if (it->time < current_time - ITEM_UPDATE_INTERVAL) {
        assert((it->iflag & ITEM_SLABBED) == 0);

        if ((it->iflag & ITEM_LINKED) != 0) {
            item_unlink_q(engine, it);
            it->time = current_time;
            item_link_q(engine, it);
        }
    }
}

int do_item_replace(struct pagecache_engine *engine,
                    hash_item *it, hash_item *new_it) {
    assert((it->iflag & ITEM_SLABBED) == 0);

    do_item_unlink(engine, it);
    return do_item_link(engine, new_it);
}

/** dumps out a list of objects of each size, with granularity of 32 bytes */
/*@null@*/
static void do_item_stats_sizes(struct pagecache_engine *engine,
                                ADD_STAT add_stats, void *c) {
    int i;
    /* max 1MB object, divided into 32 bytes size buckets */
    const int num_buckets = 32768;
    unsigned int *histogram = calloc(num_buckets, sizeof(int));

    if (histogram != NULL) {
        /* build the histogram */
        hash_item *iter = engine->items.heads;
        while (iter) {
            int ntotal = ITEM_ntotal(engine, iter);
            int bucket = ntotal / 32;
            if ((ntotal % 32) != 0) bucket++;
            if (bucket < num_buckets) histogram[bucket]++;
            iter = iter->next;
        }

        /* write the buffer */
        for (i = 0; i < num_buckets; i++) {
            if (histogram[i] != 0) {
                char key[8], val[32];
                ssize_t klen, vlen;
                klen = snprintf(key, sizeof(key), "%d", i * 32);
                vlen = snprintf(val, sizeof(val), "%u", histogram[i]);
                assert(klen < (ssize_t)sizeof(key));
                assert(vlen < (ssize_t)sizeof(val));
                add_stats(key, klen, val, vlen, c);
            }
        }
        free(histogram);
    }
    add_stats(NULL, 0, NULL, 0, c);
}

/** wrapper around assoc_find which does the lazy expiration logic */
hash_item *do_item_get(struct pagecache_engine *engine,
                       const char *key, const size_t nkey) {
    rel_time_t current_time = engine->server.core->get_current_time();
    hash_item *it = assoc_find(engine, engine->server.core->hash(key, nkey, 0), key, nkey);
    int was_found = 0;

    if (engine->config.verbose > 2) {
        if (it == NULL) {
            fprintf(stderr, "> NOT FOUND %s", key);
        } else {
            fprintf(stderr, "> FOUND KEY %s", (const char*)item_get_key(it));
            was_found++;
        }
    }

    if (it != NULL && engine->config.oldest_live != 0 &&
        engine->config.oldest_live <= current_time &&
        it->time <= engine->config.oldest_live) {
        do_item_unlink(engine, it);           /* MTSAFE - cache_lock held */
        it = NULL;
    }

    if (it == NULL && was_found) {
        fprintf(stderr, " -nuked by flush");
        was_found--;
    }

    if (it != NULL && it->exptime != 0 && it->exptime <= current_time) {
        do_item_unlink(engine, it);           /* MTSAFE - cache_lock held */
        it = NULL;
    }

    if (it == NULL && was_found) {
        fprintf(stderr, " -nuked by expire");
        was_found--;
    }

    if (it != NULL) {
        it->refcount++;
    }

    if (engine->config.verbose > 2)
        fprintf(stderr, "\n");

    return it;
}

/*
 * Stores an item in the cache according to the semantics of one of the set
 * commands. In threaded mode, this is protected by the cache lock.
 *
 * Returns the state of storage.
 */
static ENGINE_ERROR_CODE do_store_item(struct pagecache_engine *engine,
                                       hash_item *it,
                                       ENGINE_STORE_OPERATION operation,
                                       const void *cookie) {
    const char *key = item_get_key(it);
    hash_item *old_it = do_item_get(engine, key, it->nkey);
    ENGINE_ERROR_CODE stored = ENGINE_NOT_STORED;

    hash_item *new_it = NULL;

    if (old_it != NULL && operation == OPERATION_ADD) {
        /* add only adds a nonexistent item, but promote to head of LRU */
        do_item_update(engine, old_it);
    } else if (!old_it && (operation == OPERATION_REPLACE
        || operation == OPERATION_APPEND || operation == OPERATION_PREPEND))
    {
        /* replace only replaces an existing value; don't store */
    } else {
        /*
         * Append - combine new and old record into single one. Here it's
         * atomic and thread-safe.
         */
        if (operation == OPERATION_APPEND || operation == OPERATION_PREPEND) {
            if (stored == ENGINE_NOT_STORED) {
                /* we have it and old_it here - alloc memory to hold both */
                new_it = do_item_alloc(engine, key, it->nkey,
                                       old_it->flags,
                                       old_it->exptime,
                                       it->nbytes + old_it->nbytes - 2 /* CRLF */,
                                       cookie);

                if (new_it == NULL) {
                    /* SERVER_ERROR out of memory */
                    if (old_it != NULL) {
                        do_item_release(engine, old_it);
                    }

                    return ENGINE_NOT_STORED;
                }

                /* copy data from it and old_it to new_it */

                if (operation == OPERATION_APPEND) {
                    memcpy(item_get_data(new_it), item_get_data(old_it), old_it->nbytes);
                    memcpy(item_get_data(new_it) + old_it->nbytes - 2 /* CRLF */, item_get_data(it), it->nbytes);
                } else {
                    /* OPERATION_PREPEND */
                    memcpy(item_get_data(new_it), item_get_data(it), it->nbytes);
                    memcpy(item_get_data(new_it) + it->nbytes - 2 /* CRLF */, item_get_data(old_it), old_it->nbytes);
                }

                it = new_it;
            }
        }

        if (stored == ENGINE_NOT_STORED) {
            if (old_it != NULL) {
                do_item_replace(engine, old_it, it);
            } else {
                do_item_link(engine, it);
            }

            stored = ENGINE_SUCCESS;
        }
    }

    if (old_it != NULL) {
        do_item_release(engine, old_it);         /* release our reference */
    }

    if (new_it != NULL) {
        do_item_release(engine, new_it);
    }

    if (stored == ENGINE_SUCCESS) {
    }

    return stored;
}

/********************************* ITEM ACCESS *******************************/

/*
 * Allocates a new item.
 */
hash_item *item_alloc(struct pagecache_engine *engine,
                      const void *key, size_t nkey, int flags,
                      rel_time_t exptime, int nbytes, const void *cookie) {
    hash_item *it;
    pthread_mutex_lock(&engine->cache_lock);
    it = do_item_alloc(engine, key, nkey, flags, exptime, nbytes, cookie);
    pthread_mutex_unlock(&engine->cache_lock);
    return it;
}

/*
 * Returns an item if it hasn't been marked as expired,
 * lazy-expiring as needed.
 */
hash_item *item_get(struct pagecache_engine *engine,
                    const void *key, const size_t nkey) {
    hash_item *it;
    pthread_mutex_lock(&engine->cache_lock);
    it = do_item_get(engine, key, nkey);
    pthread_mutex_unlock(&engine->cache_lock);
    return it;
}

/*
 * Decrements the reference count on an item and adds it to the freelist if
 * needed.
 */
void item_release(struct pagecache_engine *engine, hash_item *item) {
    pthread_mutex_lock(&engine->cache_lock);
    do_item_release(engine, item);
    pthread_mutex_unlock(&engine->cache_lock);
}

/*
 * Unlinks an item from the LRU and hashtable.
 */
void item_unlink(struct pagecache_engine *engine, hash_item *item) {
    pthread_mutex_lock(&engine->cache_lock);
    do_item_unlink(engine, item);
    pthread_mutex_unlock(&engine->cache_lock);
}

/*
 * Stores an item in the cache (high level, obeys set/add/replace semantics)
 */
ENGINE_ERROR_CODE store_item(struct pagecache_engine *engine,
                             hash_item *item,
                             ENGINE_STORE_OPERATION operation,
                             const void *cookie) {
    ENGINE_ERROR_CODE ret;

    pthread_mutex_lock(&engine->cache_lock);
    ret = do_store_item(engine, item, operation, cookie);
    pthread_mutex_unlock(&engine->cache_lock);
    return ret;
}

/*
 * Flushes expired items after a flush_all call
 */
void item_flush_expired(struct pagecache_engine *engine, time_t when) {
    int i;
    hash_item *iter, *next;

    pthread_mutex_lock(&engine->cache_lock);

    if (when == 0) {
        engine->config.oldest_live = engine->server.core->get_current_time() - 1;
    } else {
        engine->config.oldest_live = engine->server.core->realtime(when) - 1;
    }

    if (engine->config.oldest_live != 0) {
        /*
         * The LRU is sorted in decreasing time order, and an item's
         * timestamp is never newer than its last access time, so we
         * only need to walk back until we hit an item older than the
         * oldest_live time.
         * The oldest_live checking will auto-expire the remaining items.
         */
        for (iter = engine->items.heads; iter != NULL; iter = next) {
            if (iter->time >= engine->config.oldest_live) {
                next = iter->next;
                if ((iter->iflag & ITEM_SLABBED) == 0) {
                    do_item_unlink(engine, iter);
                }
            } else {
                /* We've hit the first old item. Continue to the next queue. */
                break;
            }
        }
    }
    pthread_mutex_unlock(&engine->cache_lock);
}

void item_stats_sizes(struct pagecache_engine *engine,
                      ADD_STAT add_stat, const void *cookie)
{
    pthread_mutex_lock(&engine->cache_lock);
    do_item_stats_sizes(engine, add_stat, (void*)cookie);
    pthread_mutex_unlock(&engine->cache_lock);
}
