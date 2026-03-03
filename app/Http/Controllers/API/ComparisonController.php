<?php

namespace App\Http\Controllers\API;

use App\Http\Controllers\Controller;
use App\Models\Comparison;
use Illuminate\Http\Request;
use Illuminate\Http\JsonResponse;

class ComparisonController extends Controller
{
    public function index(Request $request): JsonResponse
    {
        $request->validate([
            'batch_id'   => 'required|exists:batches,id',
            'status'     => 'nullable|in:matched,mismatch',
            'process_no' => 'nullable|integer',
        ]);

        $query = Comparison::with(['billingSystem', 'channel', 'wallet'])
            ->where('batch_id', $request->input('batch_id'));

        if ($request->filled('status')) {
            $query->where('status', $request->input('status'));
        }

        if ($request->filled('process_no')) {
            $query->where('process_no', $request->input('process_no'));
        }

        $data = $query->orderBy('trx_date', 'asc')->paginate(50);

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
}