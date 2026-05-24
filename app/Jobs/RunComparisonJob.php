<?php

namespace App\Jobs;

use App\Models\Batch;
use App\Models\VendorTransaction;
use App\Models\BillingTransaction;
use App\Models\Comparison;
use App\Models\ComparisonHistory;
use Carbon\CarbonInterface;
use Illuminate\Bus\Queueable;
use Illuminate\Contracts\Queue\ShouldQueue;
use Illuminate\Foundation\Bus\Dispatchable;
use Illuminate\Queue\InteractsWithQueue;
use Illuminate\Queue\SerializesModels;
use Illuminate\Support\Collection;
use Illuminate\Support\Facades\Schema;

class RunComparisonJob implements ShouldQueue
{
    use Dispatchable, InteractsWithQueue, Queueable, SerializesModels;

    private const CHUNK_SIZE = 1000;

    public int $timeout = 1200; // 20 min max for large batches
    public int $tries   = 1;

    private array $previousDayVendorMismatches = [];
    private array $previousDayBillingMismatches = [];
    private array $resolvedPreviousComparisonIds = [];

    public function __construct(public Batch $batch) {}

    public function handle(): void
    {
        $batch = $this->batch;

        try {
            $processNo = ComparisonHistory::where('batch_id', $batch->id)
                ->max('process_no') ?? 0;
            $processNo++;

            // Replace old comparisons
            Comparison::where('batch_id', $batch->id)->delete();

            $this->loadPreviousDayMismatches($batch);

            $this->processVendorChunks($batch->id, $processNo);
            $this->processBillingOnlyChunks($batch->id, $processNo);
            $this->removeResolvedPreviousDayMismatches();

            $batch->update([
                'status'       => 'completed',
                'completed_at' => now(),
            ]);

        } catch (\Exception $e) {
            \Log::error("Comparison failed for batch {$batch->id}: " . $e->getMessage());
            $batch->update(['status' => 'failed']);
            throw $e;
        }
    }

    private function processVendorChunks(int $batchId, int $processNo): void
    {
        VendorTransaction::where('batch_id', $batchId)
            ->with('wallet:id,payment_channel_id')
            ->orderBy('id')
            ->chunkById(self::CHUNK_SIZE, function (Collection $vendorTrxs) use ($batchId, $processNo): void {
                $trxIds = $vendorTrxs->pluck('trx_id')->filter()->unique()->values()->all();

                $billingTrxs = BillingTransaction::where('batch_id', $batchId)
                    ->whereIn('trx_id', $trxIds)
                    ->get()
                    ->keyBy('trx_id');

                $rows = [];
                $timestamp = now();

                foreach ($vendorTrxs as $vendorTrx) {
                    $billingTrx = $billingTrxs->get($vendorTrx->trx_id);
                    $previousBillingMismatch = null;

                    if ($billingTrx === null) {
                        $previousBillingMismatch = $this->takePreviousDayBillingMismatch($vendorTrx->trx_id);
                    }

                    $matchedBillingSource = $billingTrx ?? $previousBillingMismatch;
                    $isMatched = $matchedBillingSource !== null;

                    $rows[] = [
                        'batch_id' => $batchId,
                        'process_no' => $processNo,
                        'trx_id' => $vendorTrx->trx_id,
                        'billing_system_id' => $matchedBillingSource->billing_system_id ?? null,
                        'sender_no' => $vendorTrx->sender_no,
                        'trx_date' => $vendorTrx->getRawOriginal('trx_date'),
                        'vendor_trx_date' => $vendorTrx->getRawOriginal('trx_date'),
                        'billing_trx_date' => $this->rawDateTime($matchedBillingSource, 'billing_trx_date', 'trx_date'),
                        'entity' => $matchedBillingSource->entity ?? null,
                        'customer_id' => $matchedBillingSource->customer_id ?? null,
                        'amount' => $vendorTrx->amount,
                        'channel_id' => $vendorTrx->wallet->payment_channel_id ?? null,
                        'wallet_id' => $vendorTrx->wallet_id,
                        'status' => $isMatched ? 'matched' : 'mismatch',
                        'is_vendor' => true,
                        'is_billing_system' => $isMatched,
                        'created_at' => $timestamp,
                        'updated_at' => $timestamp,
                    ];
                }

                $this->insertComparisonAndHistoryRows($rows, $batchId, $processNo, $timestamp);
            });
    }

    private function processBillingOnlyChunks(int $batchId, int $processNo): void
    {
        BillingTransaction::where('batch_id', $batchId)
            ->orderBy('id')
            ->chunkById(self::CHUNK_SIZE, function (Collection $billingTrxs) use ($batchId, $processNo): void {
                $trxIds = $billingTrxs->pluck('trx_id')->filter()->unique()->values()->all();

                $vendorTrxIdSet = VendorTransaction::where('batch_id', $batchId)
                    ->whereIn('trx_id', $trxIds)
                    ->pluck('trx_id')
                    ->flip();

                $rows = [];
                $timestamp = now();

                foreach ($billingTrxs as $billingTrx) {
                    if ($vendorTrxIdSet->has($billingTrx->trx_id)) {
                        continue;
                    }

                    $previousVendorMismatch = $this->takePreviousDayVendorMismatch($billingTrx->trx_id);
                    $isMatched = $previousVendorMismatch !== null;

                    $rows[] = [
                        'batch_id' => $batchId,
                        'process_no' => $processNo,
                        'trx_id' => $billingTrx->trx_id,
                        'billing_system_id' => $billingTrx->billing_system_id,
                        'sender_no' => $previousVendorMismatch->sender_no ?? null,
                        'trx_date' => $billingTrx->getRawOriginal('trx_date'),
                        'vendor_trx_date' => $this->rawDateTime($previousVendorMismatch, 'vendor_trx_date', 'trx_date'),
                        'billing_trx_date' => $billingTrx->getRawOriginal('trx_date'),
                        'entity' => $billingTrx->entity,
                        'customer_id' => $billingTrx->customer_id,
                        'amount' => $billingTrx->amount,
                        'channel_id' => $previousVendorMismatch->channel_id ?? null,
                        'wallet_id' => $previousVendorMismatch->wallet_id ?? null,
                        'status' => $isMatched ? 'matched' : 'mismatch',
                        'is_vendor' => $isMatched,
                        'is_billing_system' => true,
                        'created_at' => $timestamp,
                        'updated_at' => $timestamp,
                    ];
                }

                $this->insertComparisonAndHistoryRows($rows, $batchId, $processNo, $timestamp);
            });
    }

    private function insertComparisonAndHistoryRows(array $rows, int $batchId, int $processNo, $timestamp): void
    {
        if ($rows === []) {
            return;
        }

        Comparison::insert($rows);

        $historyRows = $this->buildHistoryRows($rows, $batchId, $processNo, $timestamp);

        if ($historyRows !== []) {
            ComparisonHistory::insert($historyRows);
        }
    }

    private function buildHistoryRows(array $rows, int $batchId, int $processNo, $timestamp): array
    {
        $historyTable = (new ComparisonHistory())->getTable();

        if (! Schema::hasColumn($historyTable, 'comparison_id')) {
            return $rows;
        }

        $insertedComparisons = Comparison::where('batch_id', $batchId)
            ->where('process_no', $processNo)
            ->where('created_at', $timestamp)
            ->whereIn('trx_id', array_values(array_unique(array_column($rows, 'trx_id'))))
            ->orderBy('id')
            ->get();

        if ($insertedComparisons->isEmpty()) {
            return [];
        }

        $comparisonGroups = $insertedComparisons->groupBy(fn (Comparison $comparison) => $this->historyMatchKey(
            $comparison->trx_id,
            $comparison->is_vendor,
            $comparison->is_billing_system
        ));

        $historyRows = [];
        $hasSnapshotType = Schema::hasColumn($historyTable, 'snapshot_type');

        foreach ($rows as $row) {
            $key = $this->historyMatchKey(
                $row['trx_id'],
                $row['is_vendor'],
                $row['is_billing_system']
            );

            /** @var \Illuminate\Support\Collection<int, Comparison>|null $matches */
            $matches = $comparisonGroups->get($key);

            if ($matches === null || $matches->isEmpty()) {
                continue;
            }

            /** @var Comparison $comparison */
            $comparison = $matches->shift();

            if ($matches->isEmpty()) {
                $comparisonGroups->forget($key);
            } else {
                $comparisonGroups->put($key, $matches);
            }

            $historyRow = $row;
            $historyRow['comparison_id'] = $comparison->id;

            if ($hasSnapshotType) {
                $historyRow['snapshot_type'] = 'after';
            }

            $historyRows[] = $historyRow;
        }

        return $historyRows;
    }

    private function historyMatchKey(?string $trxId, bool $isVendor, bool $isBillingSystem): string
    {
        return implode('|', [
            $trxId ?? '',
            $isVendor ? '1' : '0',
            $isBillingSystem ? '1' : '0',
        ]);
    }

    private function loadPreviousDayMismatches(Batch $batch): void
    {
        $previousDate = $batch->start_date?->copy()->subDay();

        if (! $previousDate instanceof CarbonInterface) {
            return;
        }

        $previousBatchIds = Batch::query()
            ->whereDate('start_date', $previousDate->toDateString())
            ->pluck('id');

        if ($previousBatchIds->isEmpty()) {
            return;
        }

        $mismatches = Comparison::query()
            ->whereIn('batch_id', $previousBatchIds)
            ->where('status', 'mismatch')
            ->orderBy('id')
            ->get();

        $this->previousDayVendorMismatches = $mismatches
            ->where('is_vendor', true)
            ->where('is_billing_system', false)
            ->groupBy('trx_id')
            ->map(fn (Collection $group) => $group->values()->all())
            ->all();

        $this->previousDayBillingMismatches = $mismatches
            ->where('is_vendor', false)
            ->where('is_billing_system', true)
            ->groupBy('trx_id')
            ->map(fn (Collection $group) => $group->values()->all())
            ->all();
    }

    private function takePreviousDayVendorMismatch(?string $trxId): ?Comparison
    {
        return $this->takePreviousDayMismatch($this->previousDayVendorMismatches, $trxId);
    }

    private function takePreviousDayBillingMismatch(?string $trxId): ?Comparison
    {
        return $this->takePreviousDayMismatch($this->previousDayBillingMismatches, $trxId);
    }

    private function takePreviousDayMismatch(array &$pool, ?string $trxId): ?Comparison
    {
        if ($trxId === null || ! isset($pool[$trxId][0])) {
            return null;
        }

        /** @var Comparison $comparison */
        $comparison = array_shift($pool[$trxId]);
        $this->resolvedPreviousComparisonIds[$comparison->id] = $comparison->id;

        if ($pool[$trxId] === []) {
            unset($pool[$trxId]);
        }

        return $comparison;
    }

    private function removeResolvedPreviousDayMismatches(): void
    {
        if ($this->resolvedPreviousComparisonIds === []) {
            return;
        }

        Comparison::query()
            ->whereIn('id', array_values($this->resolvedPreviousComparisonIds))
            ->delete();
    }

    private function rawDateTime(object|null $source, string $preferredField, string $fallbackField): ?string
    {
        if ($source === null) {
            return null;
        }

        if (method_exists($source, 'getRawOriginal')) {
            return $source->getRawOriginal($preferredField) ?: $source->getRawOriginal($fallbackField);
        }

        return $source->{$preferredField} ?? $source->{$fallbackField} ?? null;
    }
}
