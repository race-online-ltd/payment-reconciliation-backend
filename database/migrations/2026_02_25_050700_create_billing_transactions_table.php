<?php

use Illuminate\Database\Migrations\Migration;
use Illuminate\Database\Schema\Blueprint;
use Illuminate\Support\Facades\Schema;

return new class extends Migration
{
    /**
     * Run the migrations.
     */
    public function up(): void
    {
        Schema::create('billing_transactions', function (Blueprint $table) {
            $table->id();
            
            // Foreign key to billing_systems table, indexed for faster lookups
            $table->foreignId('billing_system_id')->index()->constrained('billing_systems')->onDelete('cascade');
            
            // Optional foreign key to batches table, set null if batch is deleted
            $table->foreignId('batch_id')->nullable()->index()->constrained('batches')->onDelete('set null');
            
            $table->string('trx_id')->index(); // Index to quickly search by transaction ID
            $table->string('entity')->nullable(); 
            $table->string('customer_id')->index(); // Index for generating reports by customer ID
            $table->string('sender_no')->nullable(); // Optional sender number
            $table->decimal('amount', 15, 2); 
            $table->dateTime('trx_date')->index(); // Index to filter transactions by date/time
            $table->timestamps(); // Laravel created_at and updated_at
        });
    }

    /**
     * Reverse the migrations.
     */
    public function down(): void
    {
        Schema::dropIfExists('billing_transactions');
    }
};
