<?php

namespace App\Services;

use Maatwebsite\Excel\Facades\Excel;
use Carbon\Carbon;
use Illuminate\Support\Facades\Log;

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

        // Normalize headers so Excel line breaks and extra spaces don't break key matching.
        $header = array_map(fn($h) => $this->normalizeHeader($h), $rows[0]);
        unset($rows[0]);

        $normalized = [];

        foreach ($rows as $index => $row) {
            $row = array_map(fn($v) => trim((string)$v), $row);
            if (count($row) !== count($header)) continue; // skip malformed rows

            $rowData = array_combine($header, $row);

            if ((int) $channelId === 2 && $index === 1) {
                Log::info('Bkash PGW raw normalized row sample', [
                    'channel_id' => $channelId,
                    'wallet_id' => $walletId,
                    'headers' => $header,
                    'rowData' => $rowData,
                ]);
            }

            $entry = null;

            switch ($channelId) {
                // case 1: // Bkash Paybill
                //     $entry = [
                //         'sender_no' => $rowData['bkash account'] ?? null,
                //         'trx_id'    => $rowData['transaction id'] ?? null,
                //         'trx_date'  => isset($rowData['transaction date']) ? Carbon::parse($rowData['transaction date'])->format('Y-m-d H:i:s') : null,
                //         'amount'    => isset($rowData['total amount']) ? floatval(str_replace(',', '', $rowData['total amount'])) : null,
                //     ];
                //     break;
                case 1: // Bkash Paybill
    $parsedDate = $this->normalizeDate($rowData['transaction date'] ?? null, [
        'Y-m-d H:i:s',
    ]);

    $entry = [
        'sender_no' => $rowData['bkash account'] ?? null,
        'trx_id'    => $rowData['transaction id'] ?? null,
        'trx_date'  => $parsedDate,
        'amount'    => isset($rowData['total amount']) ? $this->parseAmount($rowData['total amount']) : null,
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

                case 2: // Bkash PGW for automated downloads
    $parsedDate = $this->normalizeDate($this->valueFromAliases($rowData, [
        'date',
        'date time',
    ]), [
        'd-m-Y h:i A',
        'd-m-Y',
    ]);

    $entry = [
        'sender_no' => $this->valueFromAliases($rowData, [
            'from wallet',
        ]),
        'trx_id'    => $this->valueFromAliases($rowData, [
            'transaction id',
        ]),
        'trx_date'  => $parsedDate,
        'amount'    => ($amount = $this->valueFromAliases($rowData, [
            'transaction amount (in bdt)',
            'transaction amount(in bdt)',
            'transaction amount',
            'amount',
        ])) !== null ? $this->parseAmount($amount) : null,
    ];
    break;

            //    case 3: // Nagad Paybill
            //          $entry = [
            //             'sender_no' => $rowData['initiator account no.'] ?? null,
            //              'trx_id'    => $rowData['transaction id'] ?? null,
            //              'trx_date'  => isset($rowData['transaction time']) ? Carbon::parse($rowData['transaction time'])->format('Y-m-d H:i:s') : null,
            //              'amount' => isset($rowData['amount']) ? $this->parseAmount($rowData['amount']) : null,
            //          ];
            //           break;
            case 3: // Nagad Paybill for automated downloads
    $parsedDate = $this->normalizeDate($rowData['approvaldatetime'] ?? null, [
        'd-m-Y',
    ]);

    $entry = [
        'sender_no' => $rowData['initiatoraccountno'] ?? null,
        'trx_id'    => $rowData['transaction id'] ?? null,
        'trx_date'  => $parsedDate,
        'amount'    => isset($rowData['amount']) ? $this->parseAmount($rowData['amount']) : null,
    ];
    break;

                // case 4: // Nagad PGW
                //         $entry = [
                //             'sender_no' => $rowData['customer account'] ?? null,
                //             'trx_id'    => $rowData['transaction id'] ?? null,
                //             'trx_date'  => isset($rowData['transaction time']) ? Carbon::parse($rowData['transaction time'])->format('Y-m-d H:i:s') : null,
                //            'amount' => isset($rowData['amount']) ? $this->parseAmount($rowData['amount']) : null,
                //         ];
                //         break;

                case 4: // Nagad PGW for automated downloads
    $parsedDate = $this->normalizeDate($rowData['transaction time'] ?? null, [
        'd/m/Y h:i:s A',
        'd-m-Y',
    ]);

    $entry = [
        'sender_no' => $rowData['customer account'] ?? null,
        'trx_id'    => $rowData['transaction id'] ?? null,
        'trx_date'  => $parsedDate,
        'amount'    => isset($rowData['amount']) ? $this->parseAmount($rowData['amount']) : null,
    ];
    break;



                // case 5: // SSL Payment
                //     $rawTrxId = ltrim($rowData['transaction id'] ?? '', "'");
                //     $rawDate = $rowData['date time'] ?? null;
                //     $parsedDate = null;

                //     if ($rawDate) {
                //         try {
                //             if ($rawDate instanceof \DateTimeInterface) {
                //                 $parsedDate = Carbon::instance($rawDate)->format('Y-m-d H:i:s');
                //             } elseif (is_numeric($rawDate)) {
                //                 $parsedDate = Carbon::createFromTimestamp(
                //                     \PhpOffice\PhpSpreadsheet\Shared\Date::excelToTimestamp((float)$rawDate)
                //                 )->format('Y-m-d H:i:s');
                //             } else {
                //                 // Use parse() instead of createFromFormat for better flexibility
                //                 // This handles "2026-02-18 10:48:31" and "2/18/2026 10:35:55 AM"
                //                 $parsedDate = Carbon::parse(trim($rawDate))->format('Y-m-d H:i:s');
                //             }
                //         } catch (\Exception $e) {
                //             $parsedDate = null;
                //         }
                //     }

                //     $entry = [
                //         // In the CSV, 'card number' contains values like 'KXDBI48EH9FQ'
                //         'sender_no' => $rowData['card number'] ?? null, 
                //         'trx_id'    => $rawTrxId ?: null,
                //         'trx_date'  => $parsedDate,
                //         // The CSV column is 'Amount (BDT)' which becomes 'amount (bdt)' in your header mapping
                //         'amount'    => isset($rowData['amount (bdt)']) ? $this->parseAmount($rowData['amount (bdt)']) : null,
                //     ];
                //     break;

                case 5: // SSL Payment
    $parsedDate = $this->normalizeDate($rowData['date time'] ?? null, [
        'Y-m-d H:i:s',
        'd-m-Y',
    ]);

    $entry = [
        'sender_no' => $rowData['card number'] ?? null,
        'trx_id'    => ltrim($rowData['transaction id'] ?? '', "'") ?: null,
        'trx_date'  => $parsedDate,
        'amount'    => isset($rowData['amount (bdt)']) ? $this->parseAmount($rowData['amount (bdt)']) : null,
    ];
    break;
            }

            // Only keep valid rows with trx_id and amount
            if ($entry && $entry['trx_id'] && $entry['amount'] !== null) {
                $entry['wallet_id'] = $walletId;
                $entry['row_index'] = $index + 1; // optional: Excel row index
                $normalized[] = $entry;
            } elseif ((int) $channelId === 2 && $index <= 5) {
                Log::info('Bkash PGW skipped row during normalization', [
                    'channel_id' => $channelId,
                    'wallet_id' => $walletId,
                    'row_index' => $index + 1,
                    'entry' => $entry,
                    'has_trx_id' => ! empty($entry['trx_id'] ?? null),
                    'has_amount' => array_key_exists('amount', $entry ?? []) && $entry['amount'] !== null,
                ]);
            }
        }

        if ((int) $channelId === 2) {
            Log::info('Bkash PGW normalization summary', [
                'channel_id' => $channelId,
                'wallet_id' => $walletId,
                'normalized_count' => count($normalized),
                'sample_normalized_row' => $normalized[0] ?? null,
            ]);
        }

        return $normalized;
    }

    private function normalizeDate(mixed $rawDate, array $formats = []): ?string
    {
        if (blank($rawDate)) {
            return null;
        }

        if ($rawDate instanceof \DateTimeInterface) {
            return Carbon::instance($rawDate)->startOfDay()->toDateTimeString();
        }

        if (is_numeric($rawDate)) {
            return Carbon::createFromTimestamp(
                \PhpOffice\PhpSpreadsheet\Shared\Date::excelToTimestamp((float) $rawDate)
            )->startOfDay()->toDateTimeString();
        }

        $rawDate = trim((string) $rawDate);

        foreach ($formats as $format) {
            try {
                return Carbon::createFromFormat($format, $rawDate)->startOfDay()->toDateTimeString();
            } catch (\Exception) {
            }
        }

        try {
            return Carbon::parse($rawDate)->startOfDay()->toDateTimeString();
        } catch (\Exception) {
            return null;
        }
    }

    private function parseAmount(mixed $value): ?float
    {
        if (empty($value)) return null;
        $cleaned = preg_replace('/[^\d.]/i', '', str_replace(',', '', (string)$value));
        return $cleaned !== '' ? (float)$cleaned : null;
    }

    private function normalizeHeader(mixed $value): string
    {
        $value = str_replace("\xc2\xa0", ' ', (string) $value);
        $value = preg_replace('/\s+/u', ' ', $value);

        return strtolower(trim($value));
    }

    private function valueFromAliases(array $rowData, array $aliases): ?string
    {
        foreach ($aliases as $alias) {
            if (array_key_exists($alias, $rowData) && $rowData[$alias] !== '') {
                return $rowData[$alias];
            }
        }

        return null;
    }
}
