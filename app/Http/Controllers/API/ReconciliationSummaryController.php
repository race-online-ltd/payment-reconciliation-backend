<?php

namespace App\Http\Controllers\API;

use App\Http\Controllers\Controller;
use App\Models\ComparisonHistory;
use Illuminate\Http\Request;
use Illuminate\Http\JsonResponse;
use Illuminate\Support\Facades\DB;

class ReconciliationSummaryController extends Controller
{
    // channel_id => column label
    const CHANNEL_MAP = [
        1 => 'bkash_paybill',
        2 => 'bkash_pgw',
        3 => 'nagad_paybill',
        4 => 'nagad_pgw',
    ];

    public function getSummary(Request $request): JsonResponse
    {
        $request->validate([
            'start_date' => 'required|date',
            'end_date'   => 'required|date|after_or_equal:start_date',
        ]);

        // One row per (batch_id, process_no) combination
        $runs = ComparisonHistory::with('batch')
          ->whereBetween('created_at', [
                $request->input('start_date') . ' 00:00:00',
                $request->input('end_date') . ' 23:59:59',
            ])
            ->select('batch_id', 'process_no', DB::raw('MIN(created_at) as run_date'))
            ->groupBy('batch_id', 'process_no')
            ->orderBy('run_date', 'asc')
            ->get();

        $summary = [];

        foreach ($runs as $run) {
            $rows = ComparisonHistory::where('batch_id', $run->batch_id)
                ->where('process_no', $run->process_no)
                ->get();

            $total   = $rows->count();
            $matched = $rows->where('status', 'matched')->count();

            // Mismatch breakdown
            $mismatch = [
                'bkash_paybill' => 0,
                'bkash_pgw'     => 0,
                'nagad_paybill' => 0,
                'nagad_pgw'     => 0,
                'own_db'        => 0,
            ];

            foreach ($rows->where('status', 'mismatch') as $row) {
                if (!$row->is_vendor) {
                    // In billing but not in vendor
                    $mismatch['own_db']++;
                } else {
                    // In vendor but not in billing — bucket by channel
                    $label = self::CHANNEL_MAP[$row->channel_id] ?? null;
                    if ($label) $mismatch[$label]++;
                }
            }

           $summary[] = [
                'batch_id'     => $run->batch_id,
                'process_no'   => $run->process_no,
                'start_date'   => $run->batch->start_date,  // ← add this
                'end_date'     => $run->batch->end_date,    // ← add this
                'transactions' => $total,
                'matched'      => $matched,
                'mismatch'     => array_merge($mismatch, [
                    'total' => array_sum($mismatch),
                ]),
            ];
        }

        return response()->json([
            'success' => true,
            'data'    => $summary,
        ]);
    }
}