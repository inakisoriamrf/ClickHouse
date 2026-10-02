#!/usr/bin/env bash
# Tags: no-parallel
# no-parallel: the test reads the global counters of system.events.
# Checks how CACHE and SSD_CACHE dictionaries count the keys of batched updates, with one update thread and a slow source.

CURDIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CURDIR"/../shell_config.sh

$CLICKHOUSE_CLIENT -m -q "
DROP DICTIONARY IF EXISTS cache_dict_events;
DROP VIEW IF EXISTS slow_source_view;
DROP TABLE IF EXISTS source_table;

CREATE TABLE source_table (id UInt64, value String) ENGINE = MergeTree ORDER BY id;
INSERT INTO source_table SELECT number, toString(number * 3) FROM numbers(1500);

CREATE VIEW slow_source_view AS SELECT id, value FROM source_table WHERE ignore(sleep(1)) = 0;
"

function create_dictionary()
{
    local layout=$1
    $CLICKHOUSE_CLIENT -m -q "
    DROP DICTIONARY IF EXISTS cache_dict_events;
    CREATE DICTIONARY cache_dict_events (id UInt64, value String DEFAULT 'missing')
    PRIMARY KEY id
    SOURCE(CLICKHOUSE(HOST 'localhost' PORT tcpPort() USER 'default' DB '${CLICKHOUSE_DATABASE}' TABLE 'slow_source_view'))
    LIFETIME(MIN 1000 MAX 2000)
    LAYOUT($layout);
    SYSTEM RELOAD DICTIONARY cache_dict_events;
    "
}

function read_events()
{
    $CLICKHOUSE_CLIENT -q "
        SELECT
            sumIf(value, event = 'DictCacheRequests'),
            sumIf(value, event = 'DictCacheKeysRequestedDuplicate'),
            sumIf(value, event = 'DictCacheKeysRequestedFound'),
            sumIf(value, event = 'DictCacheKeysRequestedMiss'),
            sumIf(value, event = 'DictCacheKeysRequested')
        FROM system.events"
}

function print_delta()
{
    local before=$1 after=$2
    paste <(echo "$before" | tr '\t' '\n') <(echo "$after" | tr '\t' '\n') | awk '{print $2 - $1}' | paste -s -
}

echo "Keys repeat inside each unit, no key repeats across units"
# Each client requests 100 keys 30 times each. The keys of the clients are different.
# The keys 1500..1599 of the last client are not in the source.
create_dictionary "CACHE(SIZE_IN_CELLS 65536 MAX_THREADS_FOR_UPDATES 1)"
before=$(read_events)
for i in {0..15}; do
    $CLICKHOUSE_CLIENT -q "
        SELECT countIf(dictGetString('cache_dict_events', 'value', k) != if(k < 1500, toString(k * 3), 'missing'))
        FROM (SELECT $i * 100 + number % 100 AS k FROM numbers(3000))" > "${CLICKHOUSE_TMP}/events_inside_$i.out" 2>&1 &
done
wait
after=$(read_events)
cat "${CLICKHOUSE_TMP}"/events_inside_*.out | sort | uniq -c | sed -E 's/^ *[0-9]+ //'
# Duplicate keys, found keys, missing keys and requested keys: the requests depend on the timing, so they are not printed.
print_delta "$before" "$after" | cut -f 2-5
# 45000 of 48000 rows find their key in the source: the 3000 rows of the last client do not.
$CLICKHOUSE_CLIENT -q "SELECT round(found_rate, 4) FROM system.dictionaries WHERE database = currentDatabase() AND name = 'cache_dict_events'"

function check_across_units()
{
    local layout=$1
    create_dictionary "$layout"
    before=$(read_events)
    for i in {0..15}; do
        $CLICKHOUSE_CLIENT -q "
            SELECT countIf(dictGetString('cache_dict_events', 'value', k) != toString(k * 3))
            FROM (SELECT number AS k FROM numbers(1000))" > "${CLICKHOUSE_TMP}/events_across_$i.out" 2>&1 &
    done
    wait
    after=$(read_events)
    cat "${CLICKHOUSE_TMP}"/events_across_*.out | sort | uniq -c | sed -E 's/^ *[0-9]+ //'
    # Fewer source requests than units, some keys requested by more than one unit, each request finds its 1000 keys,
    # and the requested keys are the found keys plus the missing keys.
    print_delta "$before" "$after" | awk '{print ($1 < 16), ($2 > 0), ($3 == $1 * 1000), ($5 == $3 + $4)}'
}

# The first unit takes the update thread for 1 second, so the units of the other clients wait in the queue
# and are updated in a batch.
echo "All units request the same keys, CACHE"
check_across_units "CACHE(SIZE_IN_CELLS 65536 MAX_THREADS_FOR_UPDATES 1 ALLOW_READ_EXPIRED_KEYS 0)"

echo "All units request the same keys, SSD_CACHE"
# SSD_CACHE does not accept zero layout values, and ALLOW_READ_EXPIRED_KEYS is 0 by default.
# The write buffer holds all keys, so the test reads no blocks from the file (see https://github.com/ClickHouse/ClickHouse/issues/117991).
check_across_units "SSD_CACHE(BLOCK_SIZE 4096 FILE_SIZE 33554432 WRITE_BUFFER_SIZE 16777216 PATH '${USER_FILES_PATH}/${CLICKHOUSE_DATABASE}_cache_dict_events' MAX_THREADS_FOR_UPDATES 1)"

$CLICKHOUSE_CLIENT -m -q "
DROP DICTIONARY cache_dict_events;
DROP VIEW slow_source_view;
DROP TABLE source_table;
"
