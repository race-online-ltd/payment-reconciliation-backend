<?php

use Illuminate\Database\Migrations\Migration;
use Illuminate\Database\Schema\Blueprint;
use Illuminate\Support\Facades\Schema;

return new class extends Migration
{
    public function up(): void
    {
        Schema::table('wallets', function (Blueprint $table) {
            // Drop the old single-column unique index
            $table->dropUnique(['wallet_number']);

            // Add composite unique: same number CAN exist across channels,
            // but NOT within the same channel
            $table->unique(['payment_channel_id', 'wallet_number']);
        });
    }

    public function down(): void
    {
        Schema::table('wallets', function (Blueprint $table) {
            $table->dropUnique(['payment_channel_id', 'wallet_number']);
            $table->unique('wallet_number');
        });
    }
};