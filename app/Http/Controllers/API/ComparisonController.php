<?php

namespace App\Http\Controllers\API;

use App\Http\Controllers\Controller;
use App\Models\Comparison;
use Illuminate\Http\Request;
use Illuminate\Http\JsonResponse;
use App\Models\ComparisonHistory;
use Illuminate\Support\Facades\DB;


class ComparisonController extends Controller
{
    public function index(Request $request): JsonResponse
    {
       $request->validate([
        'batch_id'   => 'required|exists:batches,id',
        'status'     => 'nullable|in:matched,mismatch',
        'process_no' => 'nullable|integer',
        'channel_id' => 'nullable|integer',
        'wallet_id'  => 'nullable|integer',
    ]);

       
    $query = Comparison::with(['billingSystem', 'channel', 'wallet'])
        ->where('batch_id', $request->input('batch_id'));

    // add search function added
    if ($request->filled('search')) {
        $search = $request->input('search');
        $query->where(function ($q) use ($search) {
            $q->where('trx_id', 'LIKE', "%{$search}%")
              ->orWhere('sender_no', 'LIKE', "%{$search}%")
              ->orWhere('customer_id', 'LIKE', "%{$search}%")
              ->orWhere('entity', 'LIKE', "%{$search}%");
        });
    }

    if ($request->filled('status')) {
        $query->where('status', $request->input('status'));
    }

    if ($request->filled('process_no')) {
        $query->where('process_no', $request->input('process_no'));
    }

    if ($request->filled('channel_id')) {
        $query->where('channel_id', $request->input('channel_id'));
    }

    if ($request->filled('wallet_id')) {
        $query->where('wallet_id', $request->input('wallet_id'));
    }

    $perPage = $request->input('per_page', 50);
    $data = $query->orderBy('trx_date', 'asc')->paginate($perPage);

        return response()->json([
            'success' => true,
            'data'    => $data,
        ]);
    }

    public function show(Comparison $comparison): JsonResponse
    {
        return response()->json([
            'success' => true,
            'data'    => $comparison->load(['billingSystem', 'channel', 'wallet']),
        ]);
    }
    public function update(Request $request, Comparison $comparison): JsonResponse
    {
    $request->validate([
        'sender_no'    => 'nullable|string|max:255',
        'customer_id'  => 'nullable|string|max:255',
        'entity'       => 'nullable|string|max:255',
        'amount'       => 'nullable|numeric',
        'vendor_trx_date'   => 'nullable|date',   // ← replaced 'date'
        'billing_trx_date'  => 'nullable|date',
        'status'       => 'nullable|in:matched,mismatch',
        'own_db'       => 'nullable|boolean',
        'is_vendor'    => 'nullable|boolean',
        'channel_id'   => 'nullable|exists:payment_channels,id', 
        'wallet_id'    => 'nullable|exists:wallets,id',          
    ]);

    DB::beginTransaction();

    try {
        // 1️⃣ Snapshot current state into history BEFORE updating
        ComparisonHistory::create([
            'batch_id'          => $comparison->batch_id,
            'process_no'        => $comparison->process_no,
            'trx_id'            => $comparison->trx_id,
            'billing_system_id' => $comparison->billing_system_id,
            'sender_no'         => $comparison->sender_no,
            'trx_date'          => $comparison->trx_date,
            'vendor_trx_date'   => $comparison->getOriginal('vendor_trx_date'),   // ← add
            'billing_trx_date'  => $comparison->getOriginal('billing_trx_date'),
            'entity'            => $comparison->entity,
            'customer_id'       => $comparison->customer_id,
            'amount'            => $comparison->amount,
            'channel_id'        => $comparison->channel_id,
            'wallet_id'         => $comparison->wallet_id,
            'status'            => $comparison->status,
            'is_vendor'         => $comparison->is_vendor,
            'is_billing_system' => $comparison->is_billing_system,
        ]);

        // 2️⃣ Apply the updates
        $comparison->update([
            'sender_no'         => $request->input('sender_no',    $comparison->sender_no),
            'customer_id'       => $request->input('customer_id',  $comparison->customer_id),
            'entity'            => $request->input('entity',       $comparison->entity),
            'amount'            => $request->input('amount',       $comparison->amount),
            'vendor_trx_date'   => $request->input('vendor_trx_date',  $comparison->vendor_trx_date),   // ← add
            'billing_trx_date'  => $request->input('billing_trx_date', $comparison->billing_trx_date),
            'status'            => $request->input('status',       $comparison->status),
            'is_billing_system' => $request->input('own_db',       $comparison->is_billing_system),
            'is_vendor'         => $request->input('is_vendor',    $comparison->is_vendor),
            'channel_id'        => $request->input('channel_id',   $comparison->channel_id),  // ← add
            'wallet_id'         => $request->input('wallet_id',    $comparison->wallet_id),   // ← add

        ]);

        DB::commit();

        return response()->json([
            'success' => true,
            'data'    => $comparison->fresh()->load(['billingSystem', 'channel', 'wallet']),
        ]);

    } catch (\Exception $e) {
        DB::rollBack();

        return response()->json([
            'success' => false,
            'message' => 'Failed to update comparison',
            'error'   => $e->getMessage(),
        ], 500);
    }
}

}