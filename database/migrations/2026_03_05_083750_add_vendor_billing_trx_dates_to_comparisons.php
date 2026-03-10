<?php

use Illuminate\Database\Migrations\Migration;
use Illuminate\Database\Schema\Blueprint;
use Illuminate\Support\Facades\Schema;

return new class extends Migration
{
    public function up(): void
    {
        Schema::table('comparisons', function (Blueprint $table) {
            $table->date('vendor_trx_date')->nullable()->after('trx_date');
            $table->date('billing_trx_date')->nullable()->after('vendor_trx_date');
        });

        Schema::table('comparisons_history', function (Blueprint $table) {
            $table->date('vendor_trx_date')->nullable()->after('trx_date');
            $table->date('billing_trx_date')->nullable()->after('vendor_trx_date');
        });
    }

    public function down(): void
    {
        Schema::table('comparisons', function (Blueprint $table) {
            $table->dropColumn(['vendor_trx_date', 'billing_trx_date']);
        });

        Schema::table('comparisons_history', function (Blueprint $table) {
            $table->dropColumn(['vendor_trx_date', 'billing_trx_date']);
        });
    }
};