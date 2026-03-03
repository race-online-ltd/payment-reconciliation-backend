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

class RunComparisonJob implements ShouldQueue
{
    use Dispatchable, InteractsWithQueue, Queueable, SerializesModels;

    public int $timeout = 300; // 5 min max
    public int $tries   = 1;

    public function __construct(public Batch $batch) {}

    public function handle(): void
    {
        $batch = $this->batch;

        try {
            $processNo = ComparisonHistory::where('batch_id', $batch->id)
                ->max('process_no') ?? 0;
            $processNo++;

            // Fetch all vendor trxs with wallet (for payment_channel_id)
            $vendorTrxs = VendorTransaction::where('batch_id', $batch->id)
                ->with('wallet')
                ->get();

            // Fetch billing trxs keyed by trx_id
            $billingTrxs = BillingTransaction::where('batch_id', $batch->id)
                ->get()
                ->keyBy('trx_id');

            // Replace old comparisons
            Comparison::where('batch_id', $batch->id)->delete();

            $comparisonInsert = [];
            $historyInsert    = [];
            $processedTrxIds  = [];

            // ── Vendor vs Billing ─────────────────────────────────────────────
            foreach ($vendorTrxs as $vendorTrx) {
                $billingTrx = $billingTrxs->get($vendorTrx->trx_id);
                $isMatched  = $billingTrx !== null;

                $row = [
                    'batch_id'          => $batch->id,
                    'process_no'        => $processNo,
                    'trx_id'            => $vendorTrx->trx_id,
                    'billing_system_id' => $billingTrx->billing_system_id ?? null,
                    'sender_no'         => $vendorTrx->sender_no,
                    'trx_date'          => $vendorTrx->trx_date,
                    'entity'            => $billingTrx->entity ?? null,
                    'customer_id'       => $billingTrx->customer_id ?? null,
                    'amount'            => $vendorTrx->amount,
                    'channel_id'        => $vendorTrx->wallet->payment_channel_id ?? null,
                    'wallet_id'         => $vendorTrx->wallet_id,
                    'status'            => $isMatched ? 'matched' : 'mismatch',
                    'is_vendor'         => true,
                    'is_billing_system' => $isMatched,
                    'created_at'        => now(),
                    'updated_at'        => now(),
                ];

                $comparisonInsert[] = $row;
                $historyInsert[]    = $row;
                $processedTrxIds[]  = $vendorTrx->trx_id;
            }

            // ── Own DB mismatches (billing not in vendor) ─────────────────────
            $vendorTrxIdSet = array_flip($processedTrxIds);

            foreach ($billingTrxs as $trxId => $billingTrx) {
                if (isset($vendorTrxIdSet[$trxId])) continue;

                $row = [
                    'batch_id'          => $batch->id,
                    'process_no'        => $processNo,
                    'trx_id'            => $billingTrx->trx_id,
                    'billing_system_id' => $billingTrx->billing_system_id,
                    'sender_no'         => $billingTrx->sender_no,
                    'trx_date'          => $billingTrx->trx_date,
                    'entity'            => $billingTrx->entity,
                    'customer_id'       => $billingTrx->customer_id,
                    'amount'            => $billingTrx->amount,
                    'channel_id'        => null,
                    'wallet_id'         => null,
                    'status'            => 'mismatch',
                    'is_vendor'         => false,
                    'is_billing_system' => true,
                    'created_at'        => now(),
                    'updated_at'        => now(),
                ];

                $comparisonInsert[] = $row;
                $historyInsert[]    = $row;
            }

            // ── Bulk insert in chunks ─────────────────────────────────
            foreach (array_chunk($comparisonInsert, 300) as $chunk) {
                Comparison::insert($chunk);
            }

            foreach (array_chunk($historyInsert, 300) as $chunk) {
                ComparisonHistory::insert($chunk);
            }

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
}