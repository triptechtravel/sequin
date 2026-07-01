defmodule Sequin.CloudflareAccessTest do
  # async: false — mutates global Application env and uses the named GenServer.
  use ExUnit.Case, async: false

  alias Sequin.CloudflareAccess

  @team_domain "https://triptech.cloudflareaccess.com"
  @audience "test-audience-tag"
  @email "user@triptech.com"

  setup do
    prev = Application.get_env(:sequin, CloudflareAccess)

    Application.put_env(:sequin, CloudflareAccess,
      enabled: true,
      team_domain: @team_domain,
      audience: @audience
    )

    on_exit(fn ->
      if prev, do: Application.put_env(:sequin, CloudflareAccess, prev), else: Application.delete_env(:sequin, CloudflareAccess)
    end)

    start_supervised!(CloudflareAccess)

    # A signing keypair whose public half is seeded into the JWKS cache.
    kid = "test-kid-1"
    jwk = JOSE.JWK.generate_key({:rsa, 2048})
    CloudflareAccess.put_keys([public_jwk(jwk, kid)])

    %{jwk: jwk, kid: kid}
  end

  describe "verify_token/1" do
    test "accepts a valid token and returns claims", %{jwk: jwk, kid: kid} do
      token = sign(jwk, kid, claims())

      assert {:ok, claims} = CloudflareAccess.verify_token(token)
      assert claims["email"] == @email
      assert claims["sub"] == "cf-sub-123"
    end

    test "accepts a string (non-list) aud claim", %{jwk: jwk, kid: kid} do
      token = sign(jwk, kid, claims(%{"aud" => @audience}))
      assert {:ok, _claims} = CloudflareAccess.verify_token(token)
    end

    test "rejects an expired token", %{jwk: jwk, kid: kid} do
      token = sign(jwk, kid, claims(%{"exp" => now() - 60}))
      assert {:error, :expired} = CloudflareAccess.verify_token(token)
    end

    test "rejects a token with the wrong audience", %{jwk: jwk, kid: kid} do
      token = sign(jwk, kid, claims(%{"aud" => ["some-other-app"]}))
      assert {:error, :invalid_audience} = CloudflareAccess.verify_token(token)
    end

    test "rejects a token with the wrong issuer", %{jwk: jwk, kid: kid} do
      token = sign(jwk, kid, claims(%{"iss" => "https://evil.cloudflareaccess.com"}))
      assert {:error, :invalid_issuer} = CloudflareAccess.verify_token(token)
    end

    test "rejects a token missing the email claim", %{jwk: jwk, kid: kid} do
      token = sign(jwk, kid, claims() |> Map.delete("email"))
      assert {:error, :missing_email} = CloudflareAccess.verify_token(token)
    end

    test "rejects a token signed by a different key (bad signature)", %{kid: kid} do
      impostor = JOSE.JWK.generate_key({:rsa, 2048})
      token = sign(impostor, kid, claims())
      assert {:error, :invalid_signature} = CloudflareAccess.verify_token(token)
    end

    test "rejects a token whose kid is unknown", %{jwk: jwk} do
      token = sign(jwk, "unknown-kid", claims())
      assert {:error, reason} = CloudflareAccess.verify_token(token)
      # Refetch is attempted; with no reachable JWKS the key stays unknown.
      assert reason in [:unknown_kid, :jwks_fetch_error, :jwks_fetch_failed, :jwks_unavailable]
    end

    test "returns :not_configured when audience/team domain are unset" do
      Application.put_env(:sequin, CloudflareAccess, enabled: true, team_domain: nil, audience: nil)
      assert {:error, :not_configured} = CloudflareAccess.verify_token("anything")
    end
  end

  # --- helpers ---------------------------------------------------------------

  defp claims(overrides \\ %{}) do
    Map.merge(
      %{
        "iss" => @team_domain,
        "aud" => [@audience],
        "email" => @email,
        "sub" => "cf-sub-123",
        "name" => "Test User",
        "iat" => now(),
        "nbf" => now() - 10,
        "exp" => now() + 3600
      },
      overrides
    )
  end

  defp sign(jwk, kid, claims) do
    jwk
    |> JOSE.JWT.sign(%{"alg" => "RS256", "kid" => kid}, claims)
    |> JOSE.JWS.compact()
    |> elem(1)
  end

  defp public_jwk(jwk, kid) do
    {_modules, map} = JOSE.JWK.to_public_map(jwk)
    Map.merge(map, %{"kid" => kid, "alg" => "RS256", "use" => "sig"})
  end

  defp now, do: System.system_time(:second)
end
