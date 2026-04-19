<?php

namespace App\Services;

use Maatwebsite\Excel\Facades\Excel;
use Carbon\Carbon;

class BillingNormalizationService
{
    public function normalize(string $storedPath, ?int $billingSystemId = null): array
    {
        $fullPath = storage_path('app/private/' . $storedPath);

        if (!file_exists($fullPath)) {
            throw new \Exception("File not found at path: {$fullPath}");
        }

        $rows = Excel::toArray([], $fullPath)[0];

        if (empty($rows) || count($rows) < 2) {
            return [];
        }

        $header = array_map(fn($h) => strtolower(trim((string)$h)), $rows[0]);
        unset($rows[0]);

        $normalized = [];

        foreach ($rows as $row) {
            $row = array_map(fn($v) => trim((string)$v), $row);
            if (count($row) !== count($header)) continue;

            $rowData = array_combine($header, $row);
            if (empty(array_filter($rowData))) continue;

            $rawDate = $rowData['trx_date'] ?? null;
            $parsedDate = null;

            if ($rawDate) {
                try {
                    if ($rawDate instanceof \DateTimeInterface) {
                        $parsedDate = Carbon::instance($rawDate)->format('Y-m-d');
                    } elseif (is_numeric($rawDate)) {
                        $parsedDate = Carbon::createFromTimestamp(
                            \PhpOffice\PhpSpreadsheet\Shared\Date::excelToTimestamp((float)$rawDate)
                        )->format('Y-m-d');
                    } else {
                        $parsedDate = Carbon::createFromFormat('Y-m-d H:i:s', trim($rawDate))->format('Y-m-d');
                    }
                } catch (\Exception $e) {
                    try {
                        $parsedDate = Carbon::parse(trim($rawDate))->format('Y-m-d');
                    } catch (\Exception $e2) {
                        $parsedDate = null;
                    }
                }
            }

            $entry = [
                'trx_id'      => $rowData['trx_id'] ?? null,
                'entity'      => $rowData['entity'] ?? null,
                'customer_id' => $rowData['customer_id'] ?? null,
                'amount'      => isset($rowData['amount']) && $rowData['amount'] !== ''
                                    ? $this->parseAmount($rowData['amount'])
                                    : null,
                'trx_date'    => $parsedDate,
            ];

            if ($entry['trx_id'] && $entry['amount'] !== null) {
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