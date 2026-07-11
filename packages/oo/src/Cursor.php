<?php

declare(strict_types=1);

namespace Jhondermarck\ClickHouse;

/**
 * A streaming cursor handle. Prefer {@see rows()} to iterate the whole result
 * with bounded memory, and always {@see close()} (or use try/finally).
 */
final class Cursor
{
    public function __construct(private readonly int $id) {}

    /** Fetch the next chunk of rows (empty array once exhausted). */
    public function fetch(int $maxRows = 10000): array
    {
        return clickhouse_cursor_fetch($this->id, $maxRows);
    }

    /**
     * Iterate every row lazily, chunk by chunk — memory stays bounded by
     * $chunk regardless of result size.
     *
     * @return \Generator<int, array<string,mixed>>
     */
    public function rows(int $chunk = 10000): \Generator
    {
        while (($batch = $this->fetch($chunk)) !== []) {
            yield from $batch;
        }
    }

    public function close(): string
    {
        return clickhouse_cursor_close($this->id);
    }

    public function id(): int
    {
        return $this->id;
    }
}
