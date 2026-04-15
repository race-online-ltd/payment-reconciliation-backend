<?php

namespace App\Http\Controllers\API;

use App\Http\Controllers\Controller;
use App\Models\Wallet;
use Illuminate\Http\Request;
use Illuminate\Http\JsonResponse;
use Illuminate\Support\Facades\Validator;
use Illuminate\Validation\Rule;

class WalletController extends Controller
{
    /**
     * GET: api/wallets
     * Fetches all wallets with their nested relationships (The Deep Join).
     */
    public function index(): JsonResponse
    {
        // Eager load nested relationship: Wallet -> Channel -> Method
        // $wallets = Wallet::with('paymentChannel.paymentMethod')->get();
        $wallets = Wallet::with('paymentChannel.paymentMethod')
            ->where('status', 1)
            ->get();
        
        return response()->json([
            'success' => true,
            'data' => $wallets
        ], 200);
    }

    /**
     * POST: api/wallets
     * Validates and creates a new wallet.
     */
    public function store(Request $request): JsonResponse
    {
        $validator = Validator::make($request->all(), [
            'payment_channel_id' => 'required|exists:payment_channels,id',
            'wallet_number' => [
                    'required',
                    'string',
                    Rule::unique('wallets')->where(fn ($query) => 
                        $query->where('payment_channel_id', $request->payment_channel_id)
                    ),
                ],
            'status'             => 'required|boolean'
        ]);

        if ($validator->fails()) {
            return response()->json(['success' => false, 'errors' => $validator->errors()], 422);
        }

        $wallet = Wallet::create($request->all());

        return response()->json([
            'success' => true,
            'message' => 'Wallet created successfully',
            'data'    => $wallet->load('paymentChannel.paymentMethod')
        ], 201);
    }

    /**
     * GET: api/wallets/{wallet}
     * Fetch a specific wallet by ID with its full relationship chain.
     */
    public function show(Wallet $wallet): JsonResponse
    {
        return response()->json([
            'success' => true,
            'data' => $wallet->load('paymentChannel.paymentMethod')
        ], 200);
    }

    /**
     * PUT/PATCH: api/wallets/{wallet}
     * Update an existing wallet.
     */
    public function update(Request $request, Wallet $wallet): JsonResponse
    {
        $validator = Validator::make($request->all(), [
            'payment_channel_id' => 'sometimes|exists:payment_channels,id',
            'wallet_number' => [
                    'sometimes',
                    'string',
                    Rule::unique('wallets')
                        ->where(fn ($query) => 
                            $query->where('payment_channel_id', $request->payment_channel_id 
                                ?? $wallet->payment_channel_id)
                        )
                        ->ignore($wallet->id),
                ],
            'status'             => 'sometimes|boolean'
        ]);

        if ($validator->fails()) {
            return response()->json(['success' => false, 'errors' => $validator->errors()], 422);
        }

        $wallet->update($request->all());

        return response()->json([
            'success' => true,
            'message' => 'Wallet updated successfully',
            'data'    => $wallet->load('paymentChannel.paymentMethod')
        ], 200);
    }

    /**
     * DELETE: api/wallets/{wallet}
     * Soft or Hard delete a specific wallet.
     */
    public function destroy(Wallet $wallet): JsonResponse
    {
        $wallet->delete();

        return response()->json([
            'success' => true,
            'message' => 'Wallet deleted successfully'
        ], 200);
    }
}