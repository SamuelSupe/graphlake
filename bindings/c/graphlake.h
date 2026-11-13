#ifndef GRAPHLAKE_H
#define GRAPHLAKE_H

#include <stddef.h>
#include <stdint.h>

#ifdef __cplusplus
extern "C" {
#endif

#ifndef ARROW_C_DATA_INTERFACE
#define ARROW_C_DATA_INTERFACE
#define ARROW_FLAG_DICTIONARY_ORDERED 1
#define ARROW_FLAG_NULLABLE 2
#define ARROW_FLAG_MAP_KEYS_SORTED 4

struct ArrowSchema {
    const char *format;
    const char *name;
    const char *metadata;
    int64_t flags;
    int64_t n_children;
    struct ArrowSchema **children;
    struct ArrowSchema *dictionary;
    void (*release)(struct ArrowSchema *);
    void *private_data;
};

struct ArrowArray {
    int64_t length;
    int64_t null_count;
    int64_t offset;
    int64_t n_buffers;
    int64_t n_children;
    const void **buffers;
    struct ArrowArray **children;
    struct ArrowArray *dictionary;
    void (*release)(struct ArrowArray *);
    void *private_data;
};
#endif

#ifndef ARROW_C_STREAM_INTERFACE
#define ARROW_C_STREAM_INTERFACE
struct ArrowArrayStream {
    int (*get_schema)(struct ArrowArrayStream *, struct ArrowSchema *);
    int (*get_next)(struct ArrowArrayStream *, struct ArrowArray *);
    const char *(*get_last_error)(struct ArrowArrayStream *);
    void (*release)(struct ArrowArrayStream *);
    void *private_data;
};
#endif

typedef struct GraphLakeEngine GraphLakeEngine;

enum GraphLakeLanguage {
    GRAPHLAKE_CYPHER = 0,
    GRAPHLAKE_SQL = 1,
    GRAPHLAKE_GREMLIN = 2,
};

/* Inputs are NUL-terminated UTF-8. An optional error slot receives NULL on
 * success or a string owned by the caller on failure. Free it with
 * graphlake_error_free. Opening returns NULL on failure. Use this synchronous
 * interface and its release callbacks outside Tokio runtime threads. */
GraphLakeEngine *graphlake_engine_open(const char *catalog, char **error);

/* On success (0), writes an Arrow stream into the empty output slot. The
 * caller releases the stream and its exported schemas and batches through
 * their Arrow release callbacks. The stream can outlive the engine. Read it
 * from one thread at a time. On failure (1), the output is left untouched.
 * A zero timeout or memory limit uses the corresponding engine default. */
int graphlake_engine_query(const GraphLakeEngine *engine, int language,
                           const char *statement, uint64_t timeout_ms,
                           size_t memory_bytes, struct ArrowArrayStream *output,
                           char **error);

/* Like graphlake_engine_query, with an optional JSON object containing
 * logical_partition, snapshot_time and require_consistent_snapshot. */
int graphlake_engine_query_with_graph(const GraphLakeEngine *engine, int language,
                                      const char *statement, uint64_t timeout_ms,
                                      size_t memory_bytes, const char *graph_options,
                                      struct ArrowArrayStream *output, char **error);

/* Optional graph options and Cypher parameter JSON objects. */
int graphlake_engine_query_with_parameters(const GraphLakeEngine *engine, int language,
                                           const char *statement, uint64_t timeout_ms,
                                           size_t memory_bytes, const char *graph_options,
                                           const char *parameters,
                                           struct ArrowArrayStream *output, char **error);

void graphlake_engine_close(GraphLakeEngine *engine);
void graphlake_error_free(char *error);

#ifdef __cplusplus
}
#endif
#endif
