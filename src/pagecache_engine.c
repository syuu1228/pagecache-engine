#include "config.h"

#include <stdlib.h>
#include <stdio.h>
#include <string.h>
#include <assert.h>
#include <errno.h>
#include <ctype.h>
#include <unistd.h>
#include <stddef.h>
#include <inttypes.h>
#include <sys/mount.h>
#include <sys/mman.h>
#include <fcntl.h>

#include "pagecache_engine.h"
#include "memcached/util.h"
#include "memcached/config_parser.h"

#define CMD_SET_VBUCKET 0x83
#define CMD_GET_VBUCKET 0x84
#define CMD_DEL_VBUCKET 0x85

static const engine_info* pagecache_get_info(ENGINE_HANDLE* handle);
static ENGINE_ERROR_CODE pagecache_initialize(ENGINE_HANDLE* handle,
                                            const char* config_str);
static void pagecache_destroy(ENGINE_HANDLE* handle);
static ENGINE_ERROR_CODE pagecache_item_allocate(ENGINE_HANDLE* handle,
                                               const void* cookie,
                                               item **item,
                                               const void* key,
                                               const size_t nkey,
                                               const size_t nbytes,
                                               const int flags,
                                               const rel_time_t exptime);
static ENGINE_ERROR_CODE pagecache_item_delete(ENGINE_HANDLE* handle,
                                             const void* cookie,
                                             const void* key,
                                             const size_t nkey,
                                             uint64_t cas,
                                             uint16_t vbucket);

static void pagecache_item_release(ENGINE_HANDLE* handle, const void *cookie,
                                 item* item);
static ENGINE_ERROR_CODE pagecache_get(ENGINE_HANDLE* handle,
                                     const void* cookie,
                                     item** item,
                                     const void* key,
                                     const int nkey,
                                     uint16_t vbucket);
static ENGINE_ERROR_CODE pagecache_get_stats(ENGINE_HANDLE* handle,
                  const void *cookie,
                  const char *stat_key,
                  int nkey,
                  ADD_STAT add_stat);
static void pagecache_reset_stats(ENGINE_HANDLE* handle, const void *cookie);
static ENGINE_ERROR_CODE pagecache_store(ENGINE_HANDLE* handle,
                                       const void *cookie,
                                       item* item,
                                       uint64_t *cas,
                                       ENGINE_STORE_OPERATION operation,
                                       uint16_t vbucket);
static ENGINE_ERROR_CODE pagecache_arithmetic(ENGINE_HANDLE* handle,
                                            const void* cookie,
                                            const void* key,
                                            const int nkey,
                                            const bool increment,
                                            const bool create,
                                            const uint64_t delta,
                                            const uint64_t initial,
                                            const rel_time_t exptime,
                                            uint64_t *cas,
                                            uint64_t *result,
                                            uint16_t vbucket);
static ENGINE_ERROR_CODE pagecache_flush(ENGINE_HANDLE* handle,
                                       const void* cookie, time_t when);
static ENGINE_ERROR_CODE initalize_configuration(struct pagecache_engine *se,
                                                 const char *cfg_str);
static TAP_ITERATOR get_tap_iterator(ENGINE_HANDLE* handle, const void* cookie,
                                     const void* client, size_t nclient,
                                     uint32_t flags,
                                     const void* userdata, size_t nuserdata);
static ENGINE_ERROR_CODE pagecache_unknown_command(ENGINE_HANDLE* handle,
                                                 const void* cookie,
                                                 protocol_binary_request_header *request,
                                                 ADD_RESPONSE response);

union vbucket_info_adapter {
    char c;
    struct vbucket_info v;
};

static void set_vbucket_state(struct pagecache_engine *e,
                              uint16_t vbid, enum vbucket_state to) {
    union vbucket_info_adapter vi;
    vi.c = e->vbucket_infos[vbid];
    vi.v.state = to;
    e->vbucket_infos[vbid] = vi.c;
}

static enum vbucket_state get_vbucket_state(struct pagecache_engine *e,
                                            uint16_t vbid) {
    union vbucket_info_adapter vi;
    vi.c = e->vbucket_infos[vbid];
    return vi.v.state;
}

static bool handled_vbucket(struct pagecache_engine *e, uint16_t vbid) {
    return e->config.ignore_vbucket
        || (get_vbucket_state(e, vbid) == VBUCKET_STATE_ACTIVE);
}

/* mechanism for handling bad vbucket requests */
#define VBUCKET_GUARD(e, v) if (!handled_vbucket(e, v)) { return ENGINE_NOT_MY_VBUCKET; }

static bool get_item_info(ENGINE_HANDLE *handle, const void *cookie,
                          const item* item, item_info *item_info);

static const char const * vbucket_state_name(enum vbucket_state s) {
    static const char const * vbucket_states[] = {
        "dead", "active", "replica", "pending"
    };
    return vbucket_states[s];
}

ENGINE_ERROR_CODE create_instance(uint64_t interface,
                                  GET_SERVER_API get_server_api,
                                  ENGINE_HANDLE **handle) {
   SERVER_HANDLE_V1 *api = get_server_api();
   if (interface != 1 || api == NULL) {
      return ENGINE_ENOTSUP;
   }

   struct pagecache_engine *engine = malloc(sizeof(*engine));
   if (engine == NULL) {
      return ENGINE_ENOMEM;
   }

   struct pagecache_engine pagecache_engine = {
      .engine = {
         .interface = {
            .interface = 1
         },
         .get_info = pagecache_get_info,
         .initialize = pagecache_initialize,
         .destroy = pagecache_destroy,
         .allocate = pagecache_item_allocate,
         .remove = pagecache_item_delete,
         .release = pagecache_item_release,
         .get = pagecache_get,
         .get_stats = pagecache_get_stats,
         .reset_stats = pagecache_reset_stats,
         .store = pagecache_store,
         .arithmetic = pagecache_arithmetic,
         .flush = pagecache_flush,
         .unknown_command = pagecache_unknown_command,
         .item_set_cas = item_set_cas,
         .get_item_info = get_item_info,
         .get_tap_iterator = get_tap_iterator
      },
      .server = *api,
      .get_server_api = get_server_api,
      .initialized = true,
      .assoc = {
         .hashpower = 16,
      },
      .cache_lock = PTHREAD_MUTEX_INITIALIZER,
      .stats = {
         .lock = PTHREAD_MUTEX_INITIALIZER,
      },
      .config = {
         .use_cas = false,
         .verbose = 1,
         .oldest_live = 0,
         .evict_to_free = true,
         .maxbytes = SIZE_MAX,
         .preallocate = false,
         .factor = 1.25,
         .chunk_size = 48,
         .item_size_max= SIZE_MAX
       },
      .scrubber = {
         .lock = PTHREAD_MUTEX_INITIALIZER,
      },
      .info.engine_info = {
           .description = "pagecache engine v0.1",
           .num_features = 1,
           .features = {
               [0].feature = ENGINE_FEATURE_LRU
           }
       }
   };

   *engine = pagecache_engine;

   *handle = (ENGINE_HANDLE*)&engine->engine;
   return ENGINE_SUCCESS;
}

static inline struct pagecache_engine* get_handle(ENGINE_HANDLE* handle) {
   return (struct pagecache_engine*)handle;
}

static inline hash_item* get_real_item(item* item) {
    return (hash_item*)item;
}

static const engine_info* pagecache_get_info(ENGINE_HANDLE* handle) {
    return &get_handle(handle)->info.engine_info;
}

static ENGINE_ERROR_CODE pagecache_initialize(ENGINE_HANDLE* handle,
                                            const char* config_str) {
   struct pagecache_engine* se = get_handle(handle);

   ENGINE_ERROR_CODE ret = initalize_configuration(se, config_str);
   if (ret != ENGINE_SUCCESS) {
      return ret;
   }

   ret = assoc_init(se);
   if (ret != ENGINE_SUCCESS) {
      return ret;
   }

   ret = chdir(MEM_CACHE_PATH);
   if (ret)
      return ENGINE_FAILED;
   
   return ENGINE_SUCCESS;
}

static void pagecache_destroy(ENGINE_HANDLE* handle) {
   struct pagecache_engine* se = get_handle(handle);

   if (se->initialized) {
      pthread_mutex_destroy(&se->cache_lock);
      pthread_mutex_destroy(&se->stats.lock);
      se->initialized = false;
      free(se);
   }
}

static ENGINE_ERROR_CODE pagecache_item_allocate(ENGINE_HANDLE* handle,
                                               const void* cookie,
                                               item **item,
                                               const void* key,
                                               const size_t nkey,
                                               const size_t nbytes,
                                               const int flags,
                                               const rel_time_t exptime) {
   struct pagecache_engine* engine = get_handle(handle);
   size_t ntotal = sizeof(hash_item) + nkey + nbytes;
   hash_item *it;
   it = item_alloc(engine, key, nkey, flags, exptime, nbytes, cookie);

   if (it != NULL) {
      *item = it;
      return ENGINE_SUCCESS;
   } else {
      return ENGINE_ENOMEM;
   }
}

static ENGINE_ERROR_CODE pagecache_item_delete(ENGINE_HANDLE* handle,
                                             const void* cookie,
                                             const void* key,
                                             const size_t nkey,
                                             uint64_t cas,
                                             uint16_t vbucket)
{
   struct pagecache_engine* engine = get_handle(handle);
   VBUCKET_GUARD(engine, vbucket);

   hash_item *it = item_get(engine, key, nkey);
   if (it == NULL) {
      return ENGINE_KEY_ENOENT;
   }

   item_unlink(engine, it);
   item_release(engine, it);

   return ENGINE_SUCCESS;
}

static void pagecache_item_release(ENGINE_HANDLE* handle,
                                 const void *cookie,
                                 item* item) {
   item_release(get_handle(handle), get_real_item(item));
}

static ENGINE_ERROR_CODE pagecache_get(ENGINE_HANDLE* handle,
                                     const void* cookie,
                                     item** item,
                                     const void* key,
                                     const int nkey,
                                     uint16_t vbucket) {
   struct pagecache_engine *engine = get_handle(handle);
   VBUCKET_GUARD(engine, vbucket);

   *item = item_get(engine, key, nkey);
   if (*item != NULL) {
      return ENGINE_SUCCESS;
   } else {
      return ENGINE_KEY_ENOENT;
   }
}

static void stats_vbucket(struct pagecache_engine *e,
                          ADD_STAT add_stat,
                          const void *cookie) {
	int i;
    for (i = 0; i < NUM_VBUCKETS; i++) {
        enum vbucket_state state = get_vbucket_state(e, i);
        if (state != VBUCKET_STATE_DEAD) {
            char buf[16];
            snprintf(buf, sizeof(buf), "vb_%d", i);
            const char * state_name = vbucket_state_name(state);
            add_stat(buf, strlen(buf), state_name, strlen(state_name), cookie);
        }
    }
}

static ENGINE_ERROR_CODE pagecache_get_stats(ENGINE_HANDLE* handle,
                                           const void* cookie,
                                           const char* stat_key,
                                           int nkey,
                                           ADD_STAT add_stat)
{
   struct pagecache_engine* engine = get_handle(handle);
   ENGINE_ERROR_CODE ret = ENGINE_SUCCESS;

   if (stat_key == NULL) {
      char val[128];
      int len;

      pthread_mutex_lock(&engine->stats.lock);
      len = sprintf(val, "%"PRIu64, (uint64_t)engine->stats.evictions);
      add_stat("evictions", 9, val, len, cookie);
      len = sprintf(val, "%"PRIu64, (uint64_t)engine->stats.curr_items);
      add_stat("curr_items", 10, val, len, cookie);
      len = sprintf(val, "%"PRIu64, (uint64_t)engine->stats.total_items);
      add_stat("total_items", 11, val, len, cookie);
      len = sprintf(val, "%"PRIu64, (uint64_t)engine->stats.curr_mem_bytes);
      add_stat("mem bytes", 5, val, len, cookie);
      len = sprintf(val, "%"PRIu64, (uint64_t)engine->stats.curr_disk_bytes);
      add_stat("disk bytes", 5, val, len, cookie);
      len = sprintf(val, "%"PRIu64, engine->stats.reclaimed);
      add_stat("reclaimed", 9, val, len, cookie);
      len = sprintf(val, "%"PRIu64, (uint64_t)engine->config.maxbytes);
      add_stat("engine_maxbytes", 15, val, len, cookie);
      pthread_mutex_unlock(&engine->stats.lock);
/*
   } else if (strncmp(stat_key, "items", 5) == 0) {
      item_stats(engine, add_stat, cookie);
   } else if (strncmp(stat_key, "sizes", 5) == 0) {
      item_stats_sizes(engine, add_stat, cookie);
*/
   } else if (strncmp(stat_key, "vbucket", 7) == 0) {
      stats_vbucket(engine, add_stat, cookie);
   } else if (strncmp(stat_key, "scrub", 5) == 0) {
      char val[128];
      int len;

      pthread_mutex_lock(&engine->scrubber.lock);
      if (engine->scrubber.running) {
         add_stat("scrubber:status", 15, "running", 7, cookie);
      } else {
         add_stat("scrubber:status", 15, "stopped", 7, cookie);
      }

      if (engine->scrubber.started != 0) {
         if (engine->scrubber.stopped != 0) {
            time_t diff = engine->scrubber.started - engine->scrubber.stopped;
            len = sprintf(val, "%"PRIu64, (uint64_t)diff);
            add_stat("scrubber:last_run", 17, val, len, cookie);
         }

         len = sprintf(val, "%"PRIu64, engine->scrubber.visited);
         add_stat("scrubber:visited", 16, val, len, cookie);
         len = sprintf(val, "%"PRIu64, engine->scrubber.cleaned);
         add_stat("scrubber:cleaned", 16, val, len, cookie);
      }
      pthread_mutex_unlock(&engine->scrubber.lock);
   } else {
      ret = ENGINE_KEY_ENOENT;
   }

   return ret;
}

static ENGINE_ERROR_CODE pagecache_store(ENGINE_HANDLE* handle,
                                       const void *cookie,
                                       item* item,
                                       uint64_t *cas,
                                       ENGINE_STORE_OPERATION operation,
                                       uint16_t vbucket) {
    struct pagecache_engine *engine = get_handle(handle);
    VBUCKET_GUARD(engine, vbucket);
    return store_item(engine, get_real_item(item), operation, 
                      cookie);
}

static ENGINE_ERROR_CODE pagecache_arithmetic(ENGINE_HANDLE* handle,
                                            const void* cookie,
                                            const void* key,
                                            const int nkey,
                                            const bool increment,
                                            const bool create,
                                            const uint64_t delta,
                                            const uint64_t initial,
                                            const rel_time_t exptime,
                                            uint64_t *cas,
                                            uint64_t *result,
                                            uint16_t vbucket) {
	return ENGINE_ENOTSUP;
}

static ENGINE_ERROR_CODE pagecache_flush(ENGINE_HANDLE* handle,
                                       const void* cookie, time_t when) {
   item_flush_expired(get_handle(handle), when);

   return ENGINE_SUCCESS;
}

static void pagecache_reset_stats(ENGINE_HANDLE* handle, const void *cookie) {
   struct pagecache_engine *engine = get_handle(handle);
   item_stats_reset(engine);

   pthread_mutex_lock(&engine->stats.lock);
   engine->stats.evictions = 0;
   engine->stats.reclaimed = 0;
   engine->stats.total_items = 0;
   pthread_mutex_unlock(&engine->stats.lock);
}

static tap_event_t tap_always_pause(ENGINE_HANDLE *e,
                                    const void *cookie, item **itm, void **es,
                                    uint16_t *nes, uint8_t *ttl, uint16_t *flags,
                                    uint32_t *seqno, uint16_t *vbucket) {
    return TAP_PAUSE;
}

static tap_event_t tap_always_disconnect(ENGINE_HANDLE *e,
                                         const void *cookie, item **itm, void **es,
                                         uint16_t *nes, uint8_t *ttl, uint16_t *flags,
                                         uint32_t *seqno, uint16_t *vbucket) {
    return TAP_DISCONNECT;
}

static TAP_ITERATOR get_tap_iterator(ENGINE_HANDLE* handle, const void* cookie,
                                     const void* client, size_t nclient,
                                     uint32_t flags,
                                     const void* userdata, size_t nuserdata) {
    TAP_ITERATOR rv = tap_always_pause;
    if ((flags & TAP_CONNECT_FLAG_DUMP)
        || (flags & TAP_CONNECT_FLAG_TAKEOVER_VBUCKETS)) {
        rv = tap_always_disconnect;
    }
    return rv;
}

static ENGINE_ERROR_CODE initalize_configuration(struct pagecache_engine *se,
                                                 const char *cfg_str) {
   ENGINE_ERROR_CODE ret = ENGINE_SUCCESS;

   se->config.vb0 = true;

   if (cfg_str != NULL) {
      struct config_item items[] = {
         { .key = "use_cas",
           .datatype = DT_BOOL,
           .value.dt_bool = &se->config.use_cas },
         { .key = "verbose",
           .datatype = DT_SIZE,
           .value.dt_size = &se->config.verbose },
         { .key = "eviction",
           .datatype = DT_BOOL,
           .value.dt_bool = &se->config.evict_to_free },
         { .key = "cache_size",
           .datatype = DT_SIZE,
           .value.dt_size = &se->config.maxbytes },
         { .key = "preallocate",
           .datatype = DT_BOOL,
           .value.dt_bool = &se->config.preallocate },
         { .key = "factor",
           .datatype = DT_FLOAT,
           .value.dt_float = &se->config.factor },
         { .key = "chunk_size",
           .datatype = DT_SIZE,
           .value.dt_size = &se->config.chunk_size },
         { .key = "item_size_max",
           .datatype = DT_SIZE,
           .value.dt_size = &se->config.item_size_max },
         { .key = "ignore_vbucket",
           .datatype = DT_BOOL,
           .value.dt_bool = &se->config.ignore_vbucket },
         { .key = "vb0",
           .datatype = DT_BOOL,
           .value.dt_bool = &se->config.vb0 },
         { .key = "config_file",
           .datatype = DT_CONFIGFILE },
         { .key = NULL}
      };

      ret = se->server.core->parse_config(cfg_str, items, stderr);
   }

   if (se->config.vb0) {
       set_vbucket_state(se, 0, VBUCKET_STATE_ACTIVE);
   }

   return ENGINE_SUCCESS;
}

static protocol_binary_response_status set_vbucket(struct pagecache_engine *e,
                                                   protocol_binary_request_header *request,
                                                   const char **msg) {
    protocol_binary_request_no_extras *req =
        (protocol_binary_request_no_extras*)request;
    assert(req);

    char keyz[32];
    char valz[32];

    // Read the key.
    int keylen = ntohs(req->message.header.request.keylen);
    if (keylen >= (int)sizeof(keyz)) {
        *msg = "Key is too large.";
        return PROTOCOL_BINARY_RESPONSE_EINVAL;
    }
    memcpy(keyz, ((char*)request) + sizeof(req->message.header), keylen);
    keyz[keylen] = 0x00;

    // Read the value.
    size_t bodylen = ntohl(req->message.header.request.bodylen)
        - ntohs(req->message.header.request.keylen);
    if (bodylen >= sizeof(valz)) {
        *msg = "Value is too large.";
        return PROTOCOL_BINARY_RESPONSE_EINVAL;
    }
    memcpy(valz, (char*)request + sizeof(req->message.header)
           + keylen, bodylen);
    valz[bodylen] = 0x00;

    protocol_binary_response_status rv = PROTOCOL_BINARY_RESPONSE_SUCCESS;
    *msg = "Configured";

    enum vbucket_state state;
    if (strcmp(valz, "active") == 0) {
        state = VBUCKET_STATE_ACTIVE;
    } else if(strcmp(valz, "replica") == 0) {
        state = VBUCKET_STATE_REPLICA;
    } else if(strcmp(valz, "pending") == 0) {
        state = VBUCKET_STATE_PENDING;
    } else if(strcmp(valz, "dead") == 0) {
        state = VBUCKET_STATE_DEAD;
    } else {
        *msg = "Invalid state.";
        return PROTOCOL_BINARY_RESPONSE_EINVAL;
    }

    uint32_t vbucket = 0;
    if (!safe_strtoul(keyz, &vbucket) || vbucket > NUM_VBUCKETS) {
        *msg = "Value out of range.";
        rv = PROTOCOL_BINARY_RESPONSE_EINVAL;
    } else {
        set_vbucket_state(e, (uint16_t)vbucket, state);
    }

    return rv;
}

static protocol_binary_response_status get_vbucket(struct pagecache_engine *e,
                                                   protocol_binary_request_header *request,
                                                   const char **msg) {
    protocol_binary_request_no_extras *req =
        (protocol_binary_request_no_extras*)request;
    assert(req);

    char keyz[8]; // stringy 2^16 int

    // Read the key.
    int keylen = ntohs(req->message.header.request.keylen);
    if (keylen >= (int)sizeof(keyz)) {
        *msg   = "Key is too large.";
        return PROTOCOL_BINARY_RESPONSE_EINVAL;
    }
    memcpy(keyz, ((char*)request) + sizeof(req->message.header), keylen);
    keyz[keylen] = 0x00;

    protocol_binary_response_status rv = PROTOCOL_BINARY_RESPONSE_SUCCESS;

    uint32_t vbucket = 0;
    if (!safe_strtoul(keyz, &vbucket) || vbucket > NUM_VBUCKETS) {
        *msg = "Value out of range.";
        rv = PROTOCOL_BINARY_RESPONSE_EINVAL;
    } else {
        *msg = vbucket_state_name(get_vbucket_state(e, (uint16_t)vbucket));
    }

    return rv;
}

static protocol_binary_response_status rm_vbucket(struct pagecache_engine *e,
                                                  protocol_binary_request_header *request,
                                                  const char **msg) {
    protocol_binary_request_no_extras *req =
        (protocol_binary_request_no_extras*)request;
    assert(req);

    char keyz[8]; // stringy 2^16 int

    // Read the key.
    int keylen = ntohs(req->message.header.request.keylen);
    if (keylen >= (int)sizeof(keyz)) {
        *msg = "Key is too large.";
        return PROTOCOL_BINARY_RESPONSE_EINVAL;
    }
    memcpy(keyz, ((char*)request) + sizeof(req->message.header), keylen);
    keyz[keylen] = 0x00;

    protocol_binary_response_status rv = PROTOCOL_BINARY_RESPONSE_SUCCESS;

    uint32_t vbucket = 0;
    if (!safe_strtoul(keyz, &vbucket) || vbucket > NUM_VBUCKETS) {
        *msg = "Value out of range.";
        rv = PROTOCOL_BINARY_RESPONSE_EINVAL;
    } else {
        set_vbucket_state(e, (uint16_t)vbucket, VBUCKET_STATE_DEAD);
    }

    assert(msg);
    return rv;
}

static protocol_binary_response_status scrub_cmd(struct pagecache_engine *e,
                                                 protocol_binary_request_header *request,
                                                 const char **msg) {
    return ENGINE_ENOTSUP;
}

static ENGINE_ERROR_CODE pagecache_unknown_command(ENGINE_HANDLE* handle,
                                                 const void* cookie,
                                                 protocol_binary_request_header *request,
                                                 ADD_RESPONSE response)
{
   struct pagecache_engine* e = get_handle(handle);

    bool handled = true;
    const char *msg = NULL;
    protocol_binary_response_status res =
        PROTOCOL_BINARY_RESPONSE_UNKNOWN_COMMAND;

    switch(request->request.opcode) {
    case PROTOCOL_BINARY_CMD_SCRUB:
        res = scrub_cmd(e, request, &msg);
        break;
    case CMD_DEL_VBUCKET:
        res = rm_vbucket(e, request, &msg);
        break;
    case CMD_SET_VBUCKET:
        res = set_vbucket(e, request, &msg);
        break;
    case CMD_GET_VBUCKET:
        res = get_vbucket(e, request, &msg);
        break;
    default:
        handled = false;
        break;
    }

    bool sent = false;
    if (handled) {
        size_t msg_size = msg ? strlen(msg) : 0;
        sent = response(msg, (uint16_t)msg_size,
                        NULL, 0, NULL, 0,
                        PROTOCOL_BINARY_RAW_BYTES,
                        (uint16_t)res, 0, cookie);
    } else {
        sent = response(NULL, 0, NULL, 0, NULL, 0,
                        PROTOCOL_BINARY_RAW_BYTES,
                        PROTOCOL_BINARY_RESPONSE_UNKNOWN_COMMAND, 0, cookie);
    }

    if (sent) {
        return ENGINE_SUCCESS;
    } else {
        return ENGINE_FAILED;
    }
}

void item_set_cas(ENGINE_HANDLE *handle, const void *cookie,
                  item* item, uint64_t val)
{
}

const void* item_get_key(const hash_item* item)
{
    return (void*)(item + 1);
}

char* item_get_data(hash_item* item)
{
    return (char*)item->data;
}

static bool get_item_info(ENGINE_HANDLE *handle, const void *cookie,
                          const item* item, item_info *item_info)
{
    hash_item* it = (hash_item*)item;
    if (item_info->nvalue < 1) {
        return false;
    }
    item_info->cas = 0;
    item_info->exptime = it->exptime;
    item_info->nbytes = it->nbytes;
    item_info->flags = it->flags;
    item_info->clsid = 0;
    item_info->nkey = it->nkey;
    item_info->nvalue = 1;
    item_info->key = item_get_key(it);
    item_info->value[0].iov_base = item_get_data(it);
    item_info->value[0].iov_len = it->nbytes;
	item_info->fd = it->fd;
	if (!item_info->value[0].iov_base || !item_info->fd)
		return false;
    return true;
}
