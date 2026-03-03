<?php

use Illuminate\Database\Migrations\Migration;
use Illuminate\Database\Schema\Blueprint;
use Illuminate\Support\Facades\Schema;

return new class extends Migration
{
    public function up(): void
    {
        Schema::table('comparisons_history', function (Blueprint $table) {
            $table->unsignedBigInteger('billing_system_id')->nullable()->change();
            $table->unsignedBigInteger('channel_id')->nullable()->change();
        });
    }

    public function down(): void
    {
        Schema::table('comparisons_history', function (Blueprint $table) {
            $table->unsignedBigInteger('billing_system_id')->nullable(false)->change();
            $table->unsignedBigInteger('channel_id')->nullable(false)->change();
        });
    }
};