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

                // case 2: // Bkash PGW
                //     $entry = [
                //         'sender_no' => $rowData['from wallet'] ?? null,
                //         'trx_id'    => $rowData['transaction id'] ?? null,
                //         'trx_date'  => isset($rowData['date time']) ? Carbon::parse($rowData['date time'])->format('Y-m-d H:i:s') : null,
                //         'amount'    => isset($rowData['transaction amount']) ? floatval(str_replace(',', '', $rowData['transaction amount'])) : null,
                //     ];

                //     break;

                case 2: // Bkash PGW
    // Resolve date value from either 'date time' or 'date' column
    $rawDate = $rowData['date time'] ?? $rowData['date'] ?? null;

    // Try parsing with explicit formats before falling back to Carbon::parse
    $parsedDate = null;
    if ($rawDate) {
        foreach (['Y-m-d H:i:s', 'd-m-y H:i', 'd-m-Y H:i', 'Y-m-d'] as $format) {
            $attempt = \Carbon\Carbon::createFromFormat($format, trim($rawDate));
            if ($attempt !== false) {
                $parsedDate = $attempt->format('Y-m-d H:i:s');
                break;
            }
        }
        // Final fallback: let Carbon guess
        if (!$parsedDate) {
            try {
                $parsedDate = \Carbon\Carbon::parse($rawDate)->format('Y-m-d H:i:s');
            } catch (\Exception $e) {
                $parsedDate = null;
            }
        }
    }

    // Resolve amount from either column name variant
    $rawAmount = $rowData['transaction amount'] 
        ?? $rowData['transaction amount (in bdt)'] 
        ?? null;

    $entry = [
        'sender_no' => $rowData['from wallet'] ?? null,
        'trx_id'    => $rowData['transaction id'] ?? null,
        'trx_date'  => $parsedDate,
        'amount'    => $rawAmount !== null ? floatval(str_replace(',', '', $rawAmount)) : null,
    ];

    break;

               case 3: // Nagad Paybill
                     $entry = [
                        'sender_no' => $rowData['initiator account no.'] ?? null,
                         'trx_id'    => $rowData['transaction id'] ?? null,
                         'trx_date'  => isset($rowData['transaction time']) ? Carbon::parse($rowData['transaction time'])->format('Y-m-d H:i:s') : null,
                         'amount' => isset($rowData['amount']) ? $this->parseAmount($rowData['amount']) : null,
                     ];
                      break;

                case 4: // Nagad PGW
                        $entry = [
                            'sender_no' => $rowData['customer account'] ?? null,
                            'trx_id'    => $rowData['transaction id'] ?? null,
                            'trx_date'  => isset($rowData['transaction time']) ? Carbon::parse($rowData['transaction time'])->format('Y-m-d H:i:s') : null,
                           'amount' => isset($rowData['amount']) ? $this->parseAmount($rowData['amount']) : null,
                        ];
                        break;
                case 5: // SSL Payment
                    $rawTrxId = ltrim($rowData['transaction id'] ?? '', "'");
                    $rawDate = $rowData['date time'] ?? null;
                    $parsedDate = null;

                    if ($rawDate) {
                        try {
                            if ($rawDate instanceof \DateTimeInterface) {
                                $parsedDate = Carbon::instance($rawDate)->format('Y-m-d H:i:s');
                            } elseif (is_numeric($rawDate)) {
                                $parsedDate = Carbon::createFromTimestamp(
                                    \PhpOffice\PhpSpreadsheet\Shared\Date::excelToTimestamp((float)$rawDate)
                                )->format('Y-m-d H:i:s');
                            } else {
                                // Use parse() instead of createFromFormat for better flexibility
                                // This handles "2026-02-18 10:48:31" and "2/18/2026 10:35:55 AM"
                                $parsedDate = Carbon::parse(trim($rawDate))->format('Y-m-d H:i:s');
                            }
                        } catch (\Exception $e) {
                            $parsedDate = null;
                        }
                    }

                    $entry = [
                        // In the CSV, 'card number' contains values like 'KXDBI48EH9FQ'
                        'sender_no' => $rowData['card number'] ?? null, 
                        'trx_id'    => $rawTrxId ?: null,
                        'trx_date'  => $parsedDate,
                        // The CSV column is 'Amount (BDT)' which becomes 'amount (bdt)' in your header mapping
                        'amount'    => isset($rowData['amount (bdt)']) ? $this->parseAmount($rowData['amount (bdt)']) : null,
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
        private function parseAmount(mixed $value): ?float
        {
        if (empty($value)) return null;
        $cleaned = preg_replace('/[^\d.]/i', '', str_replace(',', '', (string)$value));
        return $cleaned !== '' ? (float)$cleaned : null;
        }
}
