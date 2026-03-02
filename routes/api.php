<?php

use Illuminate\Support\Facades\Route;
use App\Http\Controllers\API\UserController;
use App\Http\Controllers\API\AuthController;
use App\Http\Controllers\API\PaymentChannelController;
use App\Http\Controllers\API\WalletController;
use App\Http\Controllers\API\BillingSystemController;
use App\Http\Controllers\API\ReconcileController;
use App\Http\Controllers\API\ComparisonSummaryController;
use App\Http\Controllers\API\BatchController;
use App\Http\Controllers\API\BillingTransactionController;
use App\Http\Controllers\API\VendorTransactionController;

// PUBLIC routes
Route::post('/login', [AuthController::class, 'login']);

// PROTECTED routes (require token)
Route::middleware('auth:sanctum')->group(function () {
    Route::post('/logout', [AuthController::class, 'logout']);
    Route::post('/refresh', [AuthController::class, 'refresh']);
    
    // User CRUD
    Route::prefix('users')->group(function () {
        Route::get('/', [UserController::class, 'index']);
        Route::post('/', [UserController::class, 'store']);
        Route::get('{id}', [UserController::class, 'show']);
        Route::put('{id}', [UserController::class, 'update']);
        Route::delete('{id}', [UserController::class, 'destroy']);
    });

    // Example protected route
    Route::get('/profile', function () {
        return auth()->user();
    });

    // Payment Channel CRUD
    Route::prefix('payment-channels')->group(function () {
        Route::get('/', [PaymentChannelController::class, 'index']);
        Route::post('/', [PaymentChannelController::class, 'store']);
        Route::get('{paymentChannel}', [PaymentChannelController::class, 'show']);
        Route::put('{paymentChannel}', [PaymentChannelController::class, 'update']);
        Route::delete('{paymentChannel}', [PaymentChannelController::class, 'destroy']);
    });

    // Wallet CRUD
    Route::prefix('wallets')->group(function () {
        Route::get('/', [WalletController::class, 'index']);
        Route::post('/', [WalletController::class, 'store']);
        Route::get('{wallet}', [WalletController::class, 'show']);
        Route::put('{wallet}', [WalletController::class, 'update']);
        Route::delete('{wallet}', [WalletController::class, 'destroy']);
    });

    // Billing System CRUD
    Route::prefix('billing-systems')->group(function () {
        Route::get('/', [BillingSystemController::class, 'index']);
        Route::post('/', [BillingSystemController::class, 'store']);
        Route::get('{billingSystem}', [BillingSystemController::class, 'show']);
        Route::put('{billingSystem}', [BillingSystemController::class, 'update']);
        Route::delete('{billingSystem}', [BillingSystemController::class, 'destroy']);
    });


    // Billing System CRUD
    Route::prefix('billing-systems')->group(function () {
        Route::get('/', [BillingSystemController::class, 'index']);
        Route::post('/', [BillingSystemController::class, 'store']);
        Route::get('{billingSystem}', [BillingSystemController::class, 'show']);
        Route::put('{billingSystem}', [BillingSystemController::class, 'update']);
        Route::delete('{billingSystem}', [BillingSystemController::class, 'destroy']);
    });

    // Billing Transactions (Bulk Upload & CRUD)
    Route::prefix('billing-transactions')->group(function () {
        Route::get('/', [BillingTransactionController::class, 'index']);
        Route::post('/bulk-upload', [BillingTransactionController::class, 'bulkUpload']); // Batch upload method for billing transactions
        Route::get('{billingTransaction}', [BillingTransactionController::class, 'show']);
        Route::put('{billingTransaction}', [BillingTransactionController::class, 'update']);
        Route::delete('{billingTransaction}', [BillingTransactionController::class, 'destroy']);
    });

    // Vendor Transactions (Bulk Upload & CRUD)
    Route::prefix('vendor-transactions')->group(function () {
        Route::get('/', [VendorTransactionController::class, 'index']);
        Route::post('/bulk-upload', [VendorTransactionController::class, 'bulkUpload']); // Batch upload method for vendor transactions
        Route::get('{vendorTransaction}', [VendorTransactionController::class, 'show']);
        Route::put('{vendorTransaction}', [VendorTransactionController::class, 'update']);
        Route::delete('{vendorTransaction}', [VendorTransactionController::class, 'destroy']);
    });

    // Batches (Monitoring API)
    Route::prefix('batches')->group(function () {
        Route::get('/', [BatchController::class, 'index']);
        Route::get('{id}', [BatchController::class, 'show']);
        Route::delete('{batch}', [BatchController::class, 'destroy']);
    });

    // Comparison Summary (Daily breakdown)
    Route::get('/comparison-summary', [ComparisonSummaryController::class, 'summary']);
});

Route::post('/reconcile', [ReconcileController::class, 'reconcile']);
    // // without Middleware api list
    // // User CRUD
    // Route::prefix('users')->group(function () {
    //     Route::get('/', [UserController::class, 'index']);
    //     Route::post('/', [UserController::class, 'store']);
    //     Route::get('{id}', [UserController::class, 'show']);
    //     Route::put('{id}', [UserController::class, 'update']);
    //     Route::delete('{id}', [UserController::class, 'destroy']);
    // });

    // // Payment Channel CRUD
    // Route::prefix('payment-channels')->group(function () {
    //     Route::get('/', [PaymentChannelController::class, 'index']);
    //     Route::post('/', [PaymentChannelController::class, 'store']);
    //     Route::get('{paymentChannel}', [PaymentChannelController::class, 'show']);
    //     Route::put('{paymentChannel}', [PaymentChannelController::class, 'update']);
    //     Route::delete('{paymentChannel}', [PaymentChannelController::class, 'destroy']);
    // });

    // // Wallet CRUD
    // Route::prefix('wallets')->group(function () {
    //     Route::get('/', [WalletController::class, 'index']);
    //     Route::post('/', [WalletController::class, 'store']);
    //     Route::get('{wallet}', [WalletController::class, 'show']);
    //     Route::put('{wallet}', [WalletController::class, 'update']);
    //     Route::delete('{wallet}', [WalletController::class, 'destroy']);
    // });

    // // Billing System CRUD
    // Route::prefix('billing-systems')->group(function () {
    //     Route::get('/', [BillingSystemController::class, 'index']);
    //     Route::post('/', [BillingSystemController::class, 'store']);
    //     Route::get('{billingSystem}', [BillingSystemController::class, 'show']);
    //     Route::put('{billingSystem}', [BillingSystemController::class, 'update']);
    //     Route::delete('{billingSystem}', [BillingSystemController::class, 'destroy']);
    // });


    // // Billing System CRUD
    // Route::prefix('billing-systems')->group(function () {
    //     Route::get('/', [BillingSystemController::class, 'index']);
    //     Route::post('/', [BillingSystemController::class, 'store']);
    //     Route::get('{billingSystem}', [BillingSystemController::class, 'show']);
    //     Route::put('{billingSystem}', [BillingSystemController::class, 'update']);
    //     Route::delete('{billingSystem}', [BillingSystemController::class, 'destroy']);
    // });

    // // Billing Transactions (Bulk Upload & CRUD)
    // Route::prefix('billing-transactions')->group(function () {
    //     Route::get('/', [BillingTransactionController::class, 'index']);
    //     Route::post('/bulk-upload', [BillingTransactionController::class, 'bulkUpload']); // Batch upload method for billing transactions
    //     Route::get('{billingTransaction}', [BillingTransactionController::class, 'show']);
    //     Route::put('{billingTransaction}', [BillingTransactionController::class, 'update']);
    //     Route::delete('{billingTransaction}', [BillingTransactionController::class, 'destroy']);
    // });

    // // Vendor Transactions (Bulk Upload & CRUD)
    // Route::prefix('vendor-transactions')->group(function () {
    //     Route::get('/', [VendorTransactionController::class, 'index']);
    //     Route::post('/bulk-upload', [VendorTransactionController::class, 'bulkUpload']); // Batch upload method for vendor transactions
    //     Route::get('{vendorTransaction}', [VendorTransactionController::class, 'show']);
    //     Route::put('{vendorTransaction}', [VendorTransactionController::class, 'update']);
    //     Route::delete('{vendorTransaction}', [VendorTransactionController::class, 'destroy']);
    // });

    // // Batches (Monitoring API)
    // Route::prefix('batches')->group(function () {
    //     Route::get('/', [BatchController::class, 'index']);
    //     Route::get('{id}', [BatchController::class, 'show']);
    //     Route::delete('{batch}', [BatchController::class, 'destroy']);
    // });
