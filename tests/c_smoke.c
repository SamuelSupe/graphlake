#include "graphlake.h"

#include <assert.h>
#include <stdio.h>
#include <string.h>

static int64_t read_count(struct ArrowArrayStream *stream) {
    struct ArrowSchema schema = {0};
    assert(stream->get_schema(stream, &schema) == 0);
    assert(schema.n_children == 1 && strcmp(schema.children[0]->format, "l") == 0);
    schema.release(&schema);

    struct ArrowArray batch = {0};
    assert(stream->get_next(stream, &batch) == 0 && batch.release);
    assert(batch.n_children == 1 && batch.length == 1);
    struct ArrowArray *counts = batch.children[0];
    int64_t count = ((const int64_t *)counts->buffers[1])[counts->offset];
    batch.release(&batch);
    assert(stream->get_next(stream, &batch) == 0 && !batch.release);
    stream->release(stream);
    return count;
}

int main(int argc, char **argv) {
    assert(argc == 2);
    char *error = NULL;
    GraphLakeEngine *engine = graphlake_engine_open(argv[1], &error);
    if (!engine) {
        fprintf(stderr, "%s\n", error);
        graphlake_error_free(error);
        return 1;
    }

    struct ArrowArrayStream stream = {0};
    assert(graphlake_engine_query(engine, -1, "RETURN 1", 0, 0, &stream, &error) == 1);
    assert(error && !stream.release);
    graphlake_error_free(error);

    const char *options = "{\"logical_partition\":[{\"column\":\"tenant_id\",\"op\":\"eq\",\"value\":2}]}";
    assert(graphlake_engine_query_with_graph(engine, GRAPHLAKE_GREMLIN,
        "g.E().hasLabel('Transfer').count()", 0, 0, options, &stream, &error) == 0);
    assert(read_count(&stream) == 0);

    assert(graphlake_engine_query_with_parameters(engine, GRAPHLAKE_CYPHER,
        "MATCH (a:Account) WHERE a.account_id = $id RETURN count(*) AS n",
        0, 0, NULL, "{\"id\":20}", &stream, &error) == 0);
    assert(read_count(&stream) == 1);

    const char *strict = "{\"require_consistent_snapshot\":true}";
    assert(graphlake_engine_query_with_graph(engine, GRAPHLAKE_SQL,
        "SELECT count(*) AS n FROM \"Account\"", 0, 0, strict, &stream, &error) == 0);
    assert(read_count(&stream) == 3);
    assert(graphlake_engine_query_with_graph(engine, GRAPHLAKE_SQL,
        "SELECT count(*) AS n FROM \"Transfer\"", 0, 0, strict, &stream, &error) == 1);
    assert(error && strstr(error, "requires one consistent snapshot"));
    graphlake_error_free(error);

    int status = graphlake_engine_query(engine, GRAPHLAKE_SQL,
        "SELECT count(*) AS n FROM \"Transfer\"", 0, 0, &stream, &error);
    graphlake_engine_close(engine);
    if (status) {
        fprintf(stderr, "%s\n", error);
        graphlake_error_free(error);
        return 1;
    }

    assert(read_count(&stream) == 4);
    puts("C Arrow stream smoke passed");
}
