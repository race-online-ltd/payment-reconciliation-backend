<?php

namespace App\Http\Controllers\API;

use App\Http\Controllers\Controller;
use App\Models\VendorTransaction;
use App\Models\Batch;
use Illuminate\Http\Request;
use Illuminate\Http\JsonResponse;
use Illuminate\Support\Facades\DB;
use Illuminate\Support\Facades\Validator;
use Carbon\Carbon;
use App\Services\BulkInsertService;
use App\Services\VendorNormalizationService;

class VendorTransactionController extends Controller
{
    public function __construct(
        private readonly BulkInsertService $bulkInsertService
    ) {}

    /**
     * Upload Excel & Insert Vendor Transactions
     */
    public function uploadExcel(Request $request)
    {
        $request->validate([
            'file' => 'required|file|mimes:xlsx,csv',
            'wallet_id' => 'required|exists:wallets,id',
            'channel_id' => 'required|integer',
        ]);

        $file = $request->file('file');
        $storedPath = $file->store('private');

        // Normalize rows using the service
        $service = new VendorNormalizationService();
        $normalizedRows = $service->normalize($storedPath, $request->channel_id, $request->wallet_id);

        if (empty($normalizedRows)) {
            return response()->json(['success' => false, 'message' => 'No valid rows found in file'], 422);
        }

        // Insert into DB using bulkUpload
        return $this->bulkUpload(new Request(['file_data' => $normalizedRows]));
    }

    /**
     * Bulk Upload with Sequence and Batch Tracking
     */
    public function bulkUpload(Request $request): JsonResponse
    {
        $validator = Validator::make($request->all(), [
            'file_data' => 'required|array',
        ]);

        if ($validator->fails()) {
            return response()->json(['success' => false, 'errors' => $validator->errors()], 422);
        }

        $batch = Batch::create([
            'upload_date' => Carbon::today(),
            'status' => 'processing',
            'started_at' => now(),
        ]);

        try {
            $rows = $request->file_data;

            DB::beginTransaction();
            $totalCount = 0;
            $currentRow = 1;

            $batchData = [];
            foreach ($rows as $row) {
                $batchData[] = [
                    'batch_id'   => $batch->id,
                    'wallet_id'  => $row['wallet_id'],
                    'row_index'  => $currentRow++,
                    'trx_id'     => $row['trx_id'],
                    'sender_no'  => $row['sender_no'],
                    'trx_date'   => Carbon::parse($row['trx_date']),
                    'amount'     => $row['amount'],
                    'created_at' => now(),
                    'updated_at' => now(),
                ];
            }

            $totalCount += $this->bulkInsertService->insertInChunks(
                VendorTransaction::class,
                $batchData,
                [
                    'source' => 'vendor_transaction_bulk_upload',
                    'batch_id' => $batch->id,
                ]
            );

            $batch->update([
                'vendor_file_count' => $totalCount,
                'status' => 'completed',
                'completed_at' => now(),
            ]);

            DB::commit();

            return response()->json([
                'success' => true,
                'message' => "Successfully processed $totalCount records in Batch #$batch->id",
                'batch_id' => $batch->id
            ], 201);

        } catch (\Exception $e) {
            DB::rollBack();
            $batch->update(['status' => 'failed']);
            \Log::error('Vendor transaction import failed.', [
                'batch_id' => $batch->id,
                'current_row' => $currentRow,
                'error' => $e->getMessage(),
            ]);
            return response()->json(['success' => false, 'message' => "Error at row $currentRow: " . $e->getMessage()], 500);
        }
    }

    // CRUD Methods
    public function index(): JsonResponse
    {
        $data = VendorTransaction::with(['wallet', 'batch'])
            ->orderBy('row_index', 'asc')
            ->paginate(50);

        return response()->json(['success' => true, 'data' => $data]);
    }

    public function show(VendorTransaction $vendorTransaction): JsonResponse
    {
        return response()->json([
            'success' => true,
            'data' => $vendorTransaction->load(['wallet', 'batch'])
        ]);
    }

    public function update(Request $request, VendorTransaction $vendorTransaction): JsonResponse
    {
        $vendorTransaction->update($request->all());

        return response()->json(['success' => true, 'message' => 'Updated successfully']);
    }

    public function destroy(VendorTransaction $vendorTransaction): JsonResponse
    {
        $vendorTransaction->delete();

        return response()->json(['success' => true, 'message' => 'Deleted successfully']);
    }
}
