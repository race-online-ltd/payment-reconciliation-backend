<?php

namespace App\Http\Controllers\API;

use App\Http\Controllers\Controller;
use App\Models\BillingTransaction;
use App\Models\Batch;
use App\Models\BillingFile;
use Illuminate\Http\Request;
use Illuminate\Http\JsonResponse;
use Illuminate\Support\Facades\DB;
use App\Services\BulkInsertService;
use App\Services\BillingNormalizationService;
use Carbon\Carbon;

class BillingTransactionController extends Controller
{
    public function __construct(
        private readonly BulkInsertService $bulkInsertService
    ) {}

    /**
     * Upload multiple billing Excel/CSV files with batch tracking
     */
    public function uploadExcel(Request $request): JsonResponse
    {
        $request->validate([
            'files' => 'required|array',
            'files.*' => 'required|file|mimes:xlsx,csv',
            'billing_system_id' => 'required|array',
            'billing_system_id.*' => 'required|exists:billing_systems,id',
        ]);

        DB::beginTransaction();

        try {
            // 1️⃣ Create batch
            $batch = Batch::create([
                'upload_date' => Carbon::today(),
                'status' => 'processing',
                'started_at' => now(),
                'billing_file_count' => count($request->file('files')),
            ]);

            $baseFolder = "batch-{$batch->id}";
            $normalizer = new BillingNormalizationService();
            $totalCount = 0;
            // 2️⃣ Process each uploaded file
            foreach ($request->file('files') as $i => $file) {
                $storedPath = $file->store("{$baseFolder}/billing_files", 'private');

                // Save BillingFile record
                $billingFile = BillingFile::create([
                    'batch_id' => $batch->id,
                    'billing_system_id' => $request->billing_system_id[$i],
                    'original_filename' => $file->getClientOriginalName(),
                    'stored_path' => $storedPath,
                ]);

                // Normalize the file
                $normalizedRows = $normalizer->normalize($storedPath, $billingFile->billing_system_id);

                // Prepare bulk insert
                $bulkInsert = [];
                foreach ($normalizedRows as $row) {
                    $bulkInsert[] = [
                        'batch_id' => $batch->id,
                        'billing_system_id' => $billingFile->billing_system_id,
                        'trx_id' => $row['trx_id'],
                        'entity' => $row['entity'] ?? null,
                        'customer_id' => $row['customer_id'] ?? null,
                        'amount' => $row['amount'],
                        'trx_date' => $row['trx_date'] ? Carbon::parse($row['trx_date']) : null,
                        'created_at' => now(),
                        'updated_at' => now(),
                    ];
                }

                if (!empty($bulkInsert)) {
                    $totalCount += $this->bulkInsertService->insertInChunks(
                        BillingTransaction::class,
                        $bulkInsert,
                        [
                            'source' => 'billing_transaction_upload',
                            'batch_id' => $batch->id,
                            'billing_system_id' => $billingFile->billing_system_id,
                            'stored_path' => $storedPath,
                        ]
                    );
                }
            }

            // 3️⃣ Update batch
            $batch->update([
                'billing_file_count' => $totalCount,
                'status' => 'completed',
                'completed_at' => now(),
            ]);

            DB::commit();

            return response()->json([
                'success' => true,
                'message' => "Successfully processed $totalCount records in Batch #$batch->id",
                'batch_id' => $batch->id,
            ], 201);

        } catch (\Exception $e) {
            DB::rollBack();
            if (isset($batch)) {
                $batch->update(['status' => 'failed']);
            }
            \Log::error('Billing file upload failed.', [
                'batch_id' => $batch->id ?? null,
                'error' => $e->getMessage(),
            ]);
            return response()->json([
                'success' => false,
                'message' => 'Failed to upload billing files',
                'error' => $e->getMessage(),
            ], 500);
        }
    }

    
    public function index(): JsonResponse
    {
      $transactions = BillingTransaction::with(['billingSystem', 'batch'])
            ->orderBy('created_at', 'desc')
            ->paginate(50);


        return response()->json([
            'success' => true,
            'data' => $transactions,
        ]);
    }

    /**
     * Show single transaction
     */
    public function show(BillingTransaction $billingTransaction): JsonResponse
    {
        return response()->json([
            'success' => true,
            'data' => $billingTransaction->load(['billingSystem', 'batch']),
        ]);
    }

    /**
     * Update transaction
     */
    public function update(Request $request, BillingTransaction $billingTransaction): JsonResponse
    {
        $billingTransaction->update($request->all());
        return response()->json(['success' => true, 'message' => 'Updated successfully']);
    }

    /**
     * Delete transaction
     */
    public function destroy(BillingTransaction $billingTransaction): JsonResponse
    {
        $billingTransaction->delete();
        return response()->json(['success' => true, 'message' => 'Deleted successfully']);
    }
}
