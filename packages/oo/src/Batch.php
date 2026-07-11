<?php

declare(strict_types=1);

namespace Jhondermarck\ClickHouse;

/**
 * An incremental insert batch. Append chunks, {@see flush()} periodically to
 * ship buffered rows, then {@see send()} to commit — or {@see abort()}. The
 * pooled connection is held until send/abort, so always finish the batch.
 */
final class Batch
{
    public function __construct(private readonly int $id) {}

    public function append(array $values): string
    {
        return clickhouse_batch_append($this->id, $values);
    }

    public function flush(): string
    {
        return clickhouse_batch_flush($this->id);
    }

    public function send(): string
    {
        return clickhouse_batch_send($this->id);
    }

    public function abort(): string
    {
        return clickhouse_batch_abort($this->id);
    }

    public function id(): int
    {
        return $this->id;
    }
}
