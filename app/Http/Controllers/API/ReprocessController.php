<?php

namespace App\Http\Controllers\API;

use App\Http\Controllers\Controller;
use App\Models\Batch;
use App\Models\VendorFile;
use App\Models\BillingFile;
use App\Models\VendorTransaction;
use App\Models\BillingTransaction;
use App\Services\BulkInsertService;
use App\Services\BillingNormalizationService;
use App\Services\VendorNormalizationService;
use Illuminate\Http\Request;
use Illuminate\Http\JsonResponse;
use Illuminate\Support\Facades\DB;
use App\Jobs\RunComparisonJob;
use Carbon\Carbon;

class ReprocessController extends Controller
{
    public function __construct(
        private readonly BulkInsertService $bulkInsertService
    ) {}

    public function reprocess(Request $request, int $batchId): JsonResponse
    {
        $request->validate([
            'service_files'        => 'required|array',
            'service_files.*'      => 'required|file|mimes:xlsx,xls,csv',
            'service_channel_id'   => 'required|array',
            'service_channel_id.*' => 'required|exists:payment_channels,id',
            'service_wallet_id'    => 'required|array',
            'service_wallet_id.*'  => 'required|exists:wallets,id',
            'billing_files'        => 'required|array',
            'billing_files.*'      => 'required|file|mimes:xlsx,xls,csv',
            'billing_system_id'    => 'required|array',
            'billing_system_id.*'  => 'required|exists:billing_systems,id',
        ]);

        // Find existing batch — reuse its dates
        $batch = Batch::findOrFail($batchId);

        DB::beginTransaction();

        try {
            $baseFolder        = "batch-{$batch->id}/reprocess-" . now()->format('YmdHis');
            $normalizer        = new VendorNormalizationService();
            $billingNormalizer = new BillingNormalizationService();

            // 1️⃣ Delete old vendor/billing transactions for this batch
            //    (comparisons are handled inside RunComparisonJob — old ones move to history)
            VendorTransaction::where('batch_id', $batch->id)->delete();
            BillingTransaction::where('batch_id', $batch->id)->delete();

            // 2️⃣ Process new vendor/service files
            foreach ($request->file('service_files') as $i => $file) {
                $path = $file->store("{$baseFolder}/vendor_files", 'private');

                $vendorFile = VendorFile::create([
                    'batch_id'          => $batch->id,
                    'channel_id'        => $request->input('service_channel_id')[$i],
                    'wallet_id'         => $request->input('service_wallet_id')[$i],
                    'original_filename' => $file->getClientOriginalName(),
                    'stored_path'       => $path,
                ]);

                $normalizedRows = $normalizer->normalize(
                    $path,
                    $vendorFile->channel_id,
                    $vendorFile->wallet_id
                );

                if ((int) $vendorFile->channel_id === 2) {
                    \Log::info('Bkash PGW reprocess normalization result', [
                        'batch_id' => $batch->id,
                        'vendor_file_id' => $vendorFile->id,
                        'file' => $vendorFile->original_filename,
                        'stored_path' => $path,
                        'channel_id' => $vendorFile->channel_id,
                        'wallet_id' => $vendorFile->wallet_id,
                        'normalized_count' => count($normalizedRows),
                        'sample_row' => $normalizedRows[0] ?? null,
                    ]);
                }

                $bulkInsert = [];
                foreach ($normalizedRows as $index => $row) {
                    $bulkInsert[] = [
                        'batch_id'   => $batch->id,
                        'wallet_id'  => $vendorFile->wallet_id,
                        'trx_id'     => $row['trx_id'],
                        'sender_no'  => $row['sender_no'],
                        'trx_date'   => $row['trx_date'] ? Carbon::parse($row['trx_date'])->toDateTimeString() : null,
                        'amount'     => $row['amount'],
                        'row_index'  => $index + 1,
                        'created_at' => now(),
                        'updated_at' => now(),
                    ];
                }

                if ((int) $vendorFile->channel_id === 2) {
                    \Log::info('Bkash PGW reprocess insert payload', [
                        'batch_id' => $batch->id,
                        'vendor_file_id' => $vendorFile->id,
                        'rows_to_insert' => count($bulkInsert),
                        'sample_insert_row' => $bulkInsert[0] ?? null,
                    ]);
                }

                if (!empty($bulkInsert)) {
                    $this->bulkInsertService->insertInChunks(
                        VendorTransaction::class,
                        $bulkInsert,
                        [
                            'source' => 'reprocess_vendor_import',
                            'batch_id' => $batch->id,
                            'channel_id' => $vendorFile->channel_id,
                            'wallet_id' => $vendorFile->wallet_id,
                            'stored_path' => $path,
                        ]
                    );

                    if ((int) $vendorFile->channel_id === 2) {
                        \Log::info('Bkash PGW reprocess insert completed', [
                            'batch_id' => $batch->id,
                            'vendor_file_id' => $vendorFile->id,
                            'inserted_rows' => count($bulkInsert),
                        ]);
                    }
                }
            }

            // 3️⃣ Process new billing files
            foreach ($request->file('billing_files') as $i => $file) {
                $path = $file->store("{$baseFolder}/billing_files", 'private');

                $billingFile = BillingFile::create([
                    'batch_id'          => $batch->id,
                    'billing_system_id' => $request->input('billing_system_id')[$i],
                    'original_filename' => $file->getClientOriginalName(),
                    'stored_path'       => $path,
                ]);

                $normalizedRows = $billingNormalizer->normalize(
                    $path,
                    $billingFile->billing_system_id
                );

                $bulkInsert = [];
                foreach ($normalizedRows as $index => $row) {
                    $bulkInsert[] = [
                        'batch_id'          => $batch->id,
                        'billing_system_id' => $billingFile->billing_system_id,
                        'trx_id'            => $row['trx_id'],
                        'entity'            => $row['entity'] ?? null,
                        'customer_id'       => $row['customer_id'] ?? null,
                        'amount'            => $row['amount'],
                        'trx_date'          => $row['trx_date'] ? Carbon::parse($row['trx_date'])->toDateTimeString() : null,
                        'created_at'        => now(),
                        'updated_at'        => now(),
                    ];
                }

                if (!empty($bulkInsert)) {
                    $this->bulkInsertService->insertInChunks(
                        BillingTransaction::class,
                        $bulkInsert,
                        [
                            'source' => 'reprocess_billing_import',
                            'batch_id' => $batch->id,
                            'billing_system_id' => $billingFile->billing_system_id,
                            'stored_path' => $path,
                        ]
                    );
                }
            }

            // 4️⃣ Mark batch as re-processing
            $batch->update(['status' => 'pending']);

            DB::commit();

            // 5️⃣ Dispatch comparison job — it will:
            //    - move current comparisons → comparison_history
            //    - insert new comparisons with incremented process_no
            RunComparisonJob::dispatch($batch);

            return response()->json([
                'success'  => true,
                'message'  => 'Reprocess started. New comparison is running in background.',
                'batch_id' => $batch->id,
            ]);

        } catch (\Exception $e) {
            DB::rollBack();
            \Log::error('Reprocess import failed.', [
                'batch_id' => $batch->id,
                'error' => $e->getMessage(),
            ]);

            return response()->json([
                'success' => false,
                'message' => 'Reprocess failed',
                'error'   => $e->getMessage(),
            ], 500);
        }
    }
}
