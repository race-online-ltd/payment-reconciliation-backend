<?php

namespace App\Http\Controllers\API;

use App\Http\Controllers\Controller;
use Illuminate\Http\Request;
use App\Models\User;
use Illuminate\Support\Facades\Hash;
use Illuminate\Validation\ValidationException;

class AuthController extends Controller
{
    // Login method
    public function login(Request $request)
    {
        $request->validate([
            'username' => 'required|string',
            'password' => 'required|string',
        ]);

        $user = User::where('username', $request->username)->first();

        if (! $user || ! $this->passwordMatches($user, $request->password)) {
            throw ValidationException::withMessages([
                'username' => ['Invalid credentials.'],
            ]);
        }

        // Create token
        $token = $user->createToken('api-token')->plainTextToken;

        return response()->json([
            'user' => $user,
            'token' => $token,
        ]);
    }

    // Logout method
    public function logout(Request $request)
    {
        $request->user()->currentAccessToken()->delete();

        return response()->json([
            'message' => 'Logged out successfully'
        ]);
    }

    protected function passwordMatches(User $user, string $plainPassword): bool
    {
        $storedHash = $user->password;

        if ($this->isLegacyBcryptHash($storedHash)) {
            $normalizedHash = '$2y$'.substr($storedHash, 4);

            if (! Hash::check($plainPassword, $normalizedHash)) {
                return false;
            }

            // Persist the normalized bcrypt prefix so future logins use the standard format.
            $user->forceFill([
                'password' => $normalizedHash,
            ])->save();

            return true;
        }

        return Hash::check($plainPassword, $storedHash);
    }

    protected function isLegacyBcryptHash(?string $hash): bool
    {
        return is_string($hash) && str_starts_with($hash, '$2a$');
    }
}
