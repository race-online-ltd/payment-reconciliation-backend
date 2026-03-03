<?php

namespace App\Services;

use Maatwebsite\Excel\Facades\Excel;
use Carbon\Carbon;

class VendorNormalizationService
{
    /**
     * Normalize vendor Excel data
     *
     * @param string $storedPath
     * @param int $channelId
     * @param int $walletId
     * @return array
     */
    public function normalize($storedPath, $channelId, $walletId)
    {
        $rows = Excel::toArray([], storage_path('app/private/' . $storedPath))[0];

        // Normalize header: lowercase & trim
        $header = array_map(fn($h) => strtolower(trim($h)), $rows[0]);
        unset($rows[0]);

        $normalized = [];

        foreach ($rows as $index => $row) {
            $row = array_map(fn($v) => trim((string)$v), $row);
            if (count($row) !== count($header)) continue; // skip malformed rows

            $rowData = array_combine($header, $row);

            // Also lowercase keys just in case
            $rowData = array_change_key_case($rowData, CASE_LOWER);

            $entry = null;

            switch ($channelId) {
                case 1: // Bkash Paybill
                    $entry = [
                        'sender_no' => $rowData['bkash account'] ?? null,
                        'trx_id'    => $rowData['transaction id'] ?? null,
                        'trx_date'  => isset($rowData['transaction date']) ? Carbon::parse($rowData['transaction date'])->format('Y-m-d H:i:s') : null,
                        'amount'    => isset($rowData['total amount']) ? floatval(str_replace(',', '', $rowData['total amount'])) : null,
                    ];
                    break;

                case 2: // Bkash PGW
                    $entry = [
                        'sender_no' => $rowData['from wallet'] ?? null,
                        'trx_id'    => $rowData['transaction id'] ?? null,
                        'trx_date'  => isset($rowData['date time']) ? Carbon::parse($rowData['date time'])->format('Y-m-d H:i:s') : null,
                        'amount'    => isset($rowData['transaction amount']) ? floatval(str_replace(',', '', $rowData['transaction amount'])) : null,
                    ];

                    break;

               case 3: // Nagad Paybill
                     $entry = [
                        'sender_no' => $rowData['initiator account no.'] ?? null,
                         'trx_id'    => $rowData['transaction id'] ?? null,
                         'trx_date'  => isset($rowData['transaction time']) ? Carbon::parse($rowData['transaction time'])->format('Y-m-d H:i:s') : null,
                         'amount'    => isset($rowData['amount']) ? floatval(str_replace(',', '', $rowData['amount'])) : null,
                     ];
                      break;

                case 4: // Nagad PGW
                        $entry = [
                            'sender_no' => $rowData['customer account'] ?? null,
                            'trx_id'    => $rowData['transaction id'] ?? null,
                            'trx_date'  => isset($rowData['transaction time']) ? Carbon::parse($rowData['transaction time'])->format('Y-m-d H:i:s') : null,
                            'amount'    => isset($rowData['amount']) ? floatval(str_replace(',', '', $rowData['amount'])) : null,
                        ];
                        break;
}

            // Only keep valid rows with trx_id and amount
            if ($entry && $entry['trx_id'] && $entry['amount'] !== null) {
                $entry['wallet_id'] = $walletId;
                $entry['row_index'] = $index + 1; // optional: Excel row index
                $normalized[] = $entry;
            }
        }

        return $normalized;
    }
}
