<?php

namespace App\Services;

use Illuminate\Support\Facades\Log;
use Throwable;

class BulkInsertService
{
    /**
     * Keep chunk sizes comfortably below MySQL's prepared statement placeholder cap.
     * 500 rows is a safe default for these import tables (roughly 7-9 columns each),
     * while still reducing query overhead for 10k+ row imports.
     */
    private const DEFAULT_CHUNK_SIZE = 500;

    /**
     * @param class-string $modelClass
     * @param array<int, array<string, mixed>> $rows
     * @param array<string, mixed> $context
     */
    public function insertInChunks(
        string $modelClass,
        array $rows,
        array $context = [],
        int $chunkSize = self::DEFAULT_CHUNK_SIZE
    ): int {
        if (empty($rows)) {
            return 0;
        }

        $totalInserted = 0;

        foreach (array_chunk($rows, $chunkSize) as $chunkIndex => $chunk) {
            try {
                $modelClass::insert($chunk);
                $totalInserted += count($chunk);
            } catch (Throwable $e) {
                Log::error('Bulk insert failed during import chunk.', [
                    'model' => $modelClass,
                    'chunk_index' => $chunkIndex,
                    'chunk_size' => count($chunk),
                    'attempted_total_rows' => count($rows),
                    'error' => $e->getMessage(),
                    'context' => $context,
                ]);

                throw $e;
            }
        }

        return $totalInserted;
    }
}
