<?php

namespace App\Jobs;

use App\Models\Batch;
use App\Models\VendorTransaction;
use App\Models\BillingTransaction;
use App\Models\Comparison;
use App\Models\ComparisonHistory;
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

            $this->processVendorChunks($batch->id, $processNo);
            $this->processBillingOnlyChunks($batch->id, $processNo);

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
                    $isMatched = $billingTrx !== null;

                    $rows[] = [
                        'batch_id' => $batchId,
                        'process_no' => $processNo,
                        'trx_id' => $vendorTrx->trx_id,
                        'billing_system_id' => $billingTrx->billing_system_id ?? null,
                        'sender_no' => $vendorTrx->sender_no,
                        'trx_date' => $vendorTrx->getRawOriginal('trx_date'),
                        'vendor_trx_date' => $vendorTrx->getRawOriginal('trx_date'),
                        'billing_trx_date' => $billingTrx?->getRawOriginal('trx_date'),
                        'entity' => $billingTrx->entity ?? null,
                        'customer_id' => $billingTrx->customer_id ?? null,
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

                    $rows[] = [
                        'batch_id' => $batchId,
                        'process_no' => $processNo,
                        'trx_id' => $billingTrx->trx_id,
                        'billing_system_id' => $billingTrx->billing_system_id,
                        'sender_no' => null,
                        'trx_date' => $billingTrx->getRawOriginal('trx_date'),
                        'vendor_trx_date' => null,
                        'billing_trx_date' => $billingTrx->getRawOriginal('trx_date'),
                        'entity' => $billingTrx->entity,
                        'customer_id' => $billingTrx->customer_id,
                        'amount' => $billingTrx->amount,
                        'channel_id' => null,
                        'wallet_id' => null,
                        'status' => 'mismatch',
                        'is_vendor' => false,
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
}
