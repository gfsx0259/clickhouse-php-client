<?php

namespace Beeterty\ClickHouse;

use Beeterty\ClickHouse\Exception\{ConnectionException, QueryException};
use Beeterty\ClickHouse\Format\Contracts\Format;
use Beeterty\ClickHouse\Format\{Csv, JsonEachRow};
use Beeterty\ClickHouse\Query\{Builder, Statement};
use Beeterty\ClickHouse\Schema\Schema;
use CurlHandle;

class Client
{
    /**
     * The cURL handle for the ClickHouse client.
     *
     * @var CurlHandle
     */
    private CurlHandle $curl;

    /**
     * Create a new ClickHouse client instance.
     *
     * @param Config $config
     */
    public function __construct(
        private readonly Config $config
    ) {
        $this->curl = curl_init();
    }

    /**
     * Return a Schema instance for DDL operations on this connection.
     *
     * @return Schema
     */
    public function schema(): Schema
    {
        return new Schema($this);
    }

    /**
     * Return a fluent query Builder scoped to the given table.
     *
     * @param string $table
     * @return Builder
     */
    public function table(string $table): Builder
    {
        return (new Builder($this))->table($table);
    }

    /**
     * Ping the ClickHouse server to check if it's reachable and responsive.
     *
     * @return bool
     */
    public function ping(): bool
    {
        try {
            $result = $this->send('SELECT 1');

            return trim($result['body']) === '1';
        } catch (\Throwable) {
            return false;
        }
    }

    /**
     * Execute a SELECT query and return a Statement wrapping the result rows.
     *
     * The SQL is sent via POST with wait_end_of_query=1 so that HTTP 200 genuinely
     * means success. Named placeholders in the form :name are substituted client-side
     * before the query is dispatched.
     *
     * Example:
     *   $stmt = $client->query('SELECT * FROM events WHERE status = :status', ['status' => 'active']);
     *   foreach ($stmt as $row) { ... }
     *
     * @see https://clickhouse.com/docs/en/interfaces/http
     *
     * @param string      $sql      SQL query string, optionally with :name placeholders.
     * @param array       $bindings Named placeholder values keyed by placeholder name.
     * @param Format|null $format   Response format — defaults to JsonEachRow.
     * @return Statement
     * @throws ConnectionException
     * @throws QueryException
     */
    public function query(string $sql, array $bindings = [], ?Format $format = null): Statement
    {
        $format ??= new JsonEachRow();

        $sql = $this->bindParams($sql, $bindings) . ' FORMAT ' . $format->name();
        $result = $this->send($sql);

        return new Statement($result['body'], $format, $result['headers']);
    }

    /**
     * Insert an array of rows into a ClickHouse table.
     *
     * Rows are encoded using the given format (default: JsonEachRow) and sent as
     * the POST body. Each row must be an associative array whose keys match the
     * target table's column names.
     *
     * Example:
     *   $client->insert('events', [
     *       ['user_id' => 1, 'event' => 'click', 'ts' => '2024-01-01 00:00:00'],
     *       ['user_id' => 2, 'event' => 'view',  'ts' => '2024-01-01 00:00:01'],
     *   ]);
     *
     * @see https://clickhouse.com/docs/en/sql-reference/statements/insert-into
     *
     * @param string      $table  Target table name.
     * @param array       $rows   Array of associative arrays — one per row.
     * @param Format|null $format Encoding format — defaults to JsonEachRow.
     * @return bool
     * @throws ConnectionException
     * @throws QueryException
     */
    public function insert(string $table, array $rows, ?Format $format = null): bool
    {
        $format ??= new JsonEachRow();

        $sql = "INSERT INTO {$table} FORMAT " . $format->name();

        $this->send($sql, $format->encode($rows));

        return true;
    }

    /**
     * Execute a DDL or DML statement (CREATE, ALTER, DROP, OPTIMIZE, etc.).
     *
     * Unlike query(), no FORMAT clause is appended and no response body is parsed.
     * Returns true on success; throws on any server or connection error.
     *
     * Example:
     *   $client->execute('OPTIMIZE TABLE events FINAL');
     *   $client->execute('ALTER TABLE events DELETE WHERE user_id = :id', ['id' => 42]);
     *
     * @see https://clickhouse.com/docs/en/interfaces/http
     *
     * @param string $sql      DDL or DML statement, optionally with :name placeholders.
     * @param array  $bindings Named placeholder values.
     * @return bool
     * @throws ConnectionException
     * @throws QueryException
     */
    public function execute(string $sql, array $bindings = []): bool
    {
        $this->send($this->bindParams($sql, $bindings));

        return true;
    }

    /**
     * Fire a DDL/DML query without waiting for it to complete.
     *
     * Uses wait_end_of_query=0 and a short read timeout so that the method
     * returns as soon as the query has been submitted. The query continues
     * running on the server (ClickHouse does not cancel write/DDL operations
     * when the HTTP client disconnects).
     *
     * Returns a query_id that can be passed to isRunning() or kill().
     *
     * Note: best suited for DDL and long-running write operations (OPTIMIZE,
     * ALTER, INSERT SELECT). SELECT queries may be cancelled server-side on
     * disconnect depending on ClickHouse's cancel_http_readonly_queries_on_client_close setting.
     *
     * @see https://clickhouse.com/docs/en/interfaces/http#query_id
     *
     * @param string $sql      DDL or DML statement, optionally with :name placeholders.
     * @param array  $bindings Named placeholder values.
     * @return string The generated query_id — pass to isRunning() or kill() to track the query.
     * @throws ConnectionException
     */
    public function executeAsync(string $sql, array $bindings = []): string
    {
        $queryId = uniqid('async_', true);
        $sql = $this->bindParams($sql, $bindings);

        $url = $this->config->dataSource() . '/?' . http_build_query([
            'database' => $this->config->database,
            'query' => $sql,
            'query_id' => $queryId,
            'wait_end_of_query' => 0,
        ]);

        $ch = curl_init();

        curl_setopt_array($ch, [
            CURLOPT_URL => $url,
            CURLOPT_POST => true,
            CURLOPT_POSTFIELDS => '',
            CURLOPT_RETURNTRANSFER => true,
            CURLOPT_CONNECTTIMEOUT => $this->config->connectTimeout,
            CURLOPT_TIMEOUT_MS => 2000,
            CURLOPT_HTTPHEADER => $this->authHeaders(),
        ]);

        curl_exec($ch);

        return $queryId;
    }

    /**
     * Return true if a query with the given query_id is still executing.
     *
     * Queries system.processes — the live table of currently running queries.
     * Returns false as soon as the query finishes or is killed.
     *
     * @see https://clickhouse.com/docs/en/operations/system-tables/processes
     *
     * @param string $queryId The query_id returned by executeAsync().
     * @return bool
     */
    public function isRunning(string $queryId): bool
    {
        return !$this->query(
            'SELECT query_id FROM system.processes WHERE query_id = :id',
            ['id' => $queryId],
        )->isEmpty();
    }

    /**
     * Kill a running query by its query_id.
     *
     * Sends a KILL QUERY statement targeting the given query_id. Returns true
     * whether or not a matching query was found — check isRunning() first if you
     * need to confirm the query was actually alive.
     *
     * @see https://clickhouse.com/docs/en/sql-reference/statements/kill#kill-query
     *
     * @param string $queryId The query_id to kill.
     * @return bool
     */
    public function kill(string $queryId): bool
    {
        $escaped = str_replace(["\\", "'"], ["\\\\", "\\'"], $queryId);

        $this->execute("KILL QUERY WHERE query_id = '{$escaped}'");

        return true;
    }

    /**
     * Execute multiple SELECT queries in parallel using curl_multi.
     *
     * Each query runs simultaneously over its own cURL handle. Pass an
     * associative array to preserve meaningful keys in the result:
     *
     *   $results = $client->parallel([
     *       'daily'   => $client->table('events')->where('period', 'day')->toSql(),
     *       'weekly'  => $client->table('events')->where('period', 'week')->toSql(),
     *   ]);
     *
     *   $results['daily']->rows();   // Statement for the first query
     *   $results['weekly']->rows();  // Statement for the second query
     *
     * @see https://clickhouse.com/docs/en/interfaces/http
     *
     * @param array<string|int, string|Builder> $queries Keyed map of SQL strings or Builder instances.
     * @param Format|null $format Response format — defaults to JsonEachRow.
     * @return array<string|int, Statement> Results keyed by the same keys as the input array.
     * @throws ConnectionException
     * @throws QueryException
     */
    public function parallel(array $queries, ?Format $format = null): array
    {
        $format ??= new JsonEachRow();

        $handles = [];

        $responseHeaders = [];

        $multi = curl_multi_init();

        foreach ($queries as $key => $query) {
            $sql = $query instanceof Builder ? $query->toSql() : $query;
            $sql .= ' FORMAT ' . $format->name();

            $url = $this->config->dataSource() . '/?' . http_build_query([
                'database'          => $this->config->database,
                'query'             => $sql,
                'wait_end_of_query' => 1,
            ]);

            $responseHeaders[$key] = [];

            $ch = curl_init();

            curl_setopt_array($ch, [
                CURLOPT_URL => $url,
                CURLOPT_POST => true,
                CURLOPT_POSTFIELDS => '',
                CURLOPT_RETURNTRANSFER => true,
                CURLOPT_TIMEOUT => $this->config->timeout,
                CURLOPT_CONNECTTIMEOUT => $this->config->connectTimeout,
                CURLOPT_HTTPHEADER => $this->authHeaders(),
                CURLOPT_ENCODING => '',
                CURLOPT_HEADERFUNCTION => function ($ch, $header) use ($key, &$responseHeaders): int {
                    $parts = explode(':', $header, 2);

                    if (\count($parts) === 2) {
                        $responseHeaders[$key][trim($parts[0])] = trim($parts[1]);
                    }

                    return \strlen($header);
                },
            ]);

            $handles[$key] = $ch;

            curl_multi_add_handle($multi, $ch);
        }

        do {
            $status = curl_multi_exec($multi, $still_running);

            if ($still_running) {
                curl_multi_select($multi);
            }
        } while ($still_running && $status === CURLM_OK);

        $results = [];

        foreach ($handles as $key => $ch) {
            $body     = curl_multi_getcontent($ch);
            $httpCode = curl_getinfo($ch, CURLINFO_HTTP_CODE);
            $error    = curl_error($ch);

            curl_multi_remove_handle($multi, $ch);

            if ($body === null || !empty($error)) {
                curl_multi_close($multi);

                throw new ConnectionException("ClickHouse parallel query [{$key}] failed: {$error}");
            }

            if ($httpCode !== 200) {
                curl_multi_close($multi);

                $query = $queries[$key];
                $sql   = $query instanceof Builder ? $query->toSql() : $query;

                throw new QueryException(
                    "ClickHouse parallel query [{$key}] failed [{$httpCode}]: {$body}",
                    $sql
                );
            }

            $results[$key] = new Statement($body, $format, $responseHeaders[$key]);
        }

        curl_multi_close($multi);

        return $results;
    }

    /**
     * Stream a local file directly into ClickHouse without loading it into memory.
     *
     * The file is read in 64 kB chunks via CURLOPT_READFUNCTION and sent as a
     * chunked-transfer POST, so even multi-gigabyte files stay memory-efficient.
     *
     * Format defaults to CSV (CSVWithNames). Any Format whose encode/decode
     * contract matches the file's on-disk structure may be passed instead.
     *
     *   $client->insertFile('events', '/data/events.csv');
     *   $client->insertFile('logs',   '/data/logs.tsv', new TabSeparated());
     *
     * @see https://clickhouse.com/docs/en/interfaces/http#inserting-data
     *
     * @param string      $table  Target table name.
     * @param string      $path   Absolute or relative path to the file to stream.
     * @param Format|null $format File format — defaults to Csv (CSVWithNames).
     * @return bool
     * @throws \RuntimeException If the file does not exist or cannot be read.
     * @throws ConnectionException
     * @throws QueryException
     */
    public function insertFile(string $table, string $path, ?Format $format = null): bool
    {
        $format ??= new Csv();

        if (!is_file($path) || !is_readable($path)) {
            throw new \RuntimeException("Cannot open file for reading: {$path}");
        }

        $fh = fopen($path, 'rb');

        if ($fh === false) {
            throw new \RuntimeException("Cannot open file for reading: {$path}");
        }

        $sql = "INSERT INTO `{$table}` FORMAT " . $format->name();

        $url = $this->config->dataSource() . '/?' . http_build_query([
            'database'          => $this->config->database,
            'query'             => $sql,
            'wait_end_of_query' => 1,
        ]);

        $responseHeaders = [];

        $headers   = $this->authHeaders();
        $headers[] = 'Transfer-Encoding: chunked';
        $headers[] = 'Expect:';

        $ch = curl_init();

        curl_setopt_array($ch, [
            CURLOPT_URL            => $url,
            CURLOPT_POST           => true,
            CURLOPT_RETURNTRANSFER => true,
            CURLOPT_TIMEOUT        => $this->config->timeout,
            CURLOPT_CONNECTTIMEOUT => $this->config->connectTimeout,
            CURLOPT_HTTPHEADER     => $headers,
            CURLOPT_ENCODING       => '',
            CURLOPT_READFUNCTION   => function ($ch, $infile, int $length) use ($fh): string {
                if ($length < 1) {
                    return '';
                }

                $chunk = fread($fh, $length);

                return $chunk === false ? '' : $chunk;
            },
            CURLOPT_HEADERFUNCTION => function ($ch, $header) use (&$responseHeaders): int {
                $parts = explode(':', $header, 2);

                if (\count($parts) === 2) {
                    $responseHeaders[trim($parts[0])] = trim($parts[1]);
                }
                return \strlen($header);
            },
        ]);

        $response = curl_exec($ch);
        $httpCode = curl_getinfo($ch, CURLINFO_HTTP_CODE);
        $error    = curl_error($ch);

        fclose($fh);

        if ($response === false || !empty($error)) {
            throw new ConnectionException("ClickHouse file insert failed: {$error}");
        }

        \assert(\is_string($response));

        if ($httpCode !== 200) {
            throw new QueryException(
                "ClickHouse file insert failed [{$httpCode}]: {$response}",
                $sql
            );
        }

        return true;
    }

    /**
     * Substitute named placeholders (:name) in a SQL string.
     *
     * @param string $sql
     * @param array  $bindings
     * @return string
     */
    private function bindParams(string $sql, array $bindings): string
    {
        if (empty($bindings)) {
            return $sql;
        }

        foreach ($bindings as $key => $value) {
            $placeholder = ':' . ltrim((string) $key, ':');
            $sql = str_replace($placeholder, $this->escape($value), $sql);
        }

        return $sql;
    }

    /**
     * Return the standard ClickHouse authentication headers used by every request.
     *
     * @return string[]
     */
    private function authHeaders(): array
    {
        return [
            'X-ClickHouse-User: ' . $this->config->username,
            'X-ClickHouse-Key: '  . $this->config->password,
            'Content-Type: text/plain',
        ];
    }

    /**
     * Escape a PHP value for safe inclusion in a ClickHouse SQL string.
     *
     * @param mixed $value
     * @return string
     */
    private function escape(mixed $value): string
    {
        return match (true) {
            $value === null  => 'NULL',
            \is_bool($value) => $value ? '1' : '0',
            \is_int($value)  => (string) $value,
            \is_float($value) => (string) $value,
            \is_array($value) => '[' . implode(', ', array_map($this->escape(...), $value)) . ']',
            default => "'" . str_replace(["\\", "'"], ["\\\\", "\\'"], (string) $value) . "'",
        };
    }

    /**
     * Execute a raw HTTP request against the ClickHouse HTTP interface.
     *
     * The SQL is always sent as the `query` URL parameter. `wait_end_of_query=1`
     * ensures the server buffers the full response before sending headers, so
     * HTTP 200 genuinely means the query succeeded. Every request uses POST —
     * ClickHouse treats GET as readonly (Code 164) and rejects DDL/DML over it.
     * For INSERT statements $body carries the row data; it is empty otherwise.
     *
     * When $config->retries > 0, connection failures are retried up to that many
     * extra times with $config->retryDelay milliseconds between attempts.
     *
     * When $config->compression is true, non-empty bodies are gzip-compressed
     * before sending. Responses are always decompressed by cURL automatically.
     *
     * @param string $sql
     * @param string $body
     * @return array{body: string, headers: array<string, string>}
     * @throws ConnectionException
     * @throws QueryException
     */
    private function send(string $sql, string $body = ''): array
    {
        $lastException = new ConnectionException('ClickHouse connection failed');

        for ($attempt = 0; $attempt <= $this->config->retries; $attempt++) {
            if ($attempt > 0) {
                usleep($this->config->retryDelay * 1000);
            }

            try {
                return $this->attempt($sql, $body);
            } catch (ConnectionException $e) {
                $lastException = $e;
            }
        }

        throw $lastException;
    }

    /**
     * Perform a single HTTP attempt.
     *
     * @param string $sql
     * @param string $body
     * @return array{body: string, headers: array<string, string>}
     * @throws ConnectionException
     * @throws QueryException
     */
    private function attempt(string $sql, string $body): array
    {
        $url = $this->config->dataSource() . '/?' . http_build_query([
            'database' => $this->config->database,
            'query' => $sql,
            'wait_end_of_query' => 1,
        ]);

        $headers = $this->authHeaders();

        if ($this->config->compression && $body !== '') {
            $compressed = gzencode($body, 1);

            if ($compressed !== false) {
                $body = $compressed;
                $headers[] = 'Content-Encoding: gzip';
            }
        }

        $responseHeaders = [];

        curl_setopt_array($this->curl, [
            CURLOPT_URL => $url,
            CURLOPT_RETURNTRANSFER => true,
            CURLOPT_TIMEOUT => $this->config->timeout,
            CURLOPT_CONNECTTIMEOUT => $this->config->connectTimeout,
            CURLOPT_HTTPHEADER => $headers,
            CURLOPT_ENCODING => '',
            CURLOPT_SSL_VERIFYPEER => false,
            CURLOPT_SSL_VERIFYHOST => false,
            CURLOPT_HEADERFUNCTION => function ($curl, $header) use (&$responseHeaders): int {
                $parts = explode(':', $header, 2);

                if (\count($parts) === 2) {
                    $responseHeaders[trim($parts[0])] = trim($parts[1]);
                }
                return \strlen($header);
            },
        ]);

        curl_setopt($this->curl, CURLOPT_POST, true);
        curl_setopt($this->curl, CURLOPT_POSTFIELDS, $body);

        $response = curl_exec($this->curl);
        $httpCode = curl_getinfo($this->curl, CURLINFO_HTTP_CODE);
        $error    = curl_error($this->curl);

        if ($response === false || !empty($error)) {
            throw new ConnectionException(
                "ClickHouse connection failed: {$error}"
            );
        }

        \assert(\is_string($response));

        if ($httpCode !== 200) {
            throw new QueryException(
                "ClickHouse query failed [{$httpCode}]: {$response}",
                $sql
            );
        }

        return ['body' => $response, 'headers' => $responseHeaders];
    }
}
