<?php

namespace App\Http\Controllers\API;

use App\Http\Controllers\Controller;
use Illuminate\Http\JsonResponse;
use Illuminate\Support\Facades\DB;

class ComparisonSummaryController extends Controller
{
    public function summary(): JsonResponse
    {
        $rows = DB::table('comparisons')
            ->selectRaw("
                DATE(trx_date) AS date,
                COUNT(*) AS transactions,
                SUM(CASE WHEN billing_system_id IS NOT NULL AND sender_no IS NOT NULL AND channel_id IS NOT NULL AND wallet_id IS NOT NULL THEN 1 ELSE 0 END) AS matched,
                SUM(CASE WHEN channel_id = 2 AND billing_system_id IS NULL THEN 1 ELSE 0 END) AS bkash_pgw,
                SUM(CASE WHEN channel_id = 1 AND billing_system_id IS NULL THEN 1 ELSE 0 END) AS bkash_paybill,
                SUM(CASE WHEN channel_id = 3 AND billing_system_id IS NULL THEN 1 ELSE 0 END) AS nagad_paybill,
                SUM(CASE WHEN channel_id = 4 AND billing_system_id IS NULL THEN 1 ELSE 0 END) AS nagad_pgw,
                SUM(CASE WHEN billing_system_id IS NOT NULL AND channel_id IS NULL AND wallet_id IS NULL THEN 1 ELSE 0 END) AS own_db,
                SUM(CASE WHEN billing_system_id IS NULL OR channel_id IS NULL OR wallet_id IS NULL THEN 1 ELSE 0 END) AS total_unmatched
            ")
            ->groupByRaw('DATE(trx_date)')
            ->orderByRaw('DATE(trx_date)')
            ->get();

        return response()->json([
            'success' => true,
            'data'    => $rows,
        ]);
    }
}
