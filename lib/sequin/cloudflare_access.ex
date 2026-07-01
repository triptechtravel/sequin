defmodule Sequin.CloudflareAccess do
  @moduledoc """
  Verifies Cloudflare Access application tokens.

  When Sequin is deployed behind Cloudflare Access, every request Cloudflare
  forwards carries a signed JWT in the `Cf-Access-Jwt-Assertion` header. This
  module trust-but-verifies that assertion: it fetches the Access application's
  public keys (JWKS) and validates the token's signature, issuer, audience and
  expiry before we trust the identity (email) it carries.

  The actual login gate (e.g. Google SSO) is enforced by Cloudflare in front of
  the hostname; this module's job is purely to verify the injected assertion so
  we never render Sequin's own sign-in screen.

  Configuration (see `config/runtime.exs`):

      config :sequin, Sequin.CloudflareAccess,
        enabled: true,
        team_domain: "https://<team>.cloudflareaccess.com",
        audience: "<application audience (AUD) tag>"

  The JWKS is cached in this GenServer and refetched at most once per
  `@refetch_cooldown_ms` when a token references an unknown key id (handles
  Cloudflare's key rotation).
  """
  use GenServer

  require Logger

  @certs_path "/cdn-cgi/access/certs"
  @allowed_algs ["RS256"]
  @refetch_cooldown_ms :timer.minutes(5)
  # Allow a little clock skew when checking not-before.
  @clock_skew_seconds 5

  # ---------------------------------------------------------------------------
  # Public API
  # ---------------------------------------------------------------------------

  def start_link(opts \\ []) do
    GenServer.start_link(__MODULE__, opts, name: __MODULE__)
  end

  @doc "Whether Cloudflare Access authentication is enabled for this deployment."
  @spec enabled?() :: boolean()
  def enabled?, do: config()[:enabled] == true

  @doc """
  Verifies a Cloudflare Access JWT.

  Returns `{:ok, claims}` (a map with at least `"email"`, and usually `"sub"`
  and `"name"`) when the token is valid, otherwise `{:error, reason}`.
  """
  @spec verify_token(String.t()) :: {:ok, map()} | {:error, atom()}
  def verify_token(token) when is_binary(token) do
    cfg = config()

    cond do
      cfg[:team_domain] in [nil, ""] -> {:error, :not_configured}
      cfg[:audience] in [nil, ""] -> {:error, :not_configured}
      true -> do_verify(token, cfg)
    end
  end

  @doc false
  # Test/support hook: seed the key cache directly with raw JWK maps so tests
  # don't need to hit the network.
  def put_keys(jwk_maps) when is_list(jwk_maps) do
    GenServer.call(__MODULE__, {:put_keys, jwk_maps})
  end

  # ---------------------------------------------------------------------------
  # GenServer
  # ---------------------------------------------------------------------------

  @impl GenServer
  def init(_opts) do
    {:ok, %{keys: %{}, last_fetch_at: nil}}
  end

  @impl GenServer
  def handle_call({:get_key, kid}, _from, state) do
    case Map.get(state.keys, kid) do
      nil ->
        case maybe_refetch(state) do
          {:ok, new_state} ->
            reply =
              case Map.fetch(new_state.keys, kid) do
                {:ok, jwk} -> {:ok, jwk}
                :error -> {:error, :unknown_kid}
              end

            {:reply, reply, new_state}

          {:error, reason, new_state} ->
            {:reply, {:error, reason}, new_state}
        end

      jwk ->
        {:reply, {:ok, jwk}, state}
    end
  end

  def handle_call({:put_keys, jwk_maps}, _from, state) do
    {:reply, :ok, %{state | keys: to_key_map(jwk_maps), last_fetch_at: now_ms()}}
  end

  # ---------------------------------------------------------------------------
  # Verification
  # ---------------------------------------------------------------------------

  defp do_verify(token, cfg) do
    with {:ok, kid} <- peek_kid(token),
         {:ok, jwk} <- GenServer.call(__MODULE__, {:get_key, kid}),
         {true, %JOSE.JWT{fields: claims}, _jws} <- JOSE.JWT.verify_strict(jwk, @allowed_algs, token),
         :ok <- validate_claims(claims, cfg) do
      {:ok, claims}
    else
      {false, _jwt, _jws} -> {:error, :invalid_signature}
      {:error, _reason} = err -> err
    end
  end

  defp peek_kid(token) do
    %{fields: fields} = JOSE.JWT.peek_protected(token)

    case fields["kid"] do
      kid when is_binary(kid) -> {:ok, kid}
      _ -> {:error, :missing_kid}
    end
  rescue
    _ -> {:error, :malformed_token}
  end

  defp validate_claims(claims, cfg) do
    now = System.system_time(:second)
    issuer = cfg[:team_domain] |> String.trim_trailing("/")
    audience = cfg[:audience]

    cond do
      is_integer(claims["exp"]) and claims["exp"] < now -> {:error, :expired}
      is_integer(claims["nbf"]) and claims["nbf"] > now + @clock_skew_seconds -> {:error, :not_yet_valid}
      claims["iss"] != issuer -> {:error, :invalid_issuer}
      not aud_match?(claims["aud"], audience) -> {:error, :invalid_audience}
      not is_binary(claims["email"]) or claims["email"] == "" -> {:error, :missing_email}
      true -> :ok
    end
  end

  defp aud_match?(aud, expected) when is_list(aud), do: expected in aud
  defp aud_match?(aud, expected) when is_binary(aud), do: aud == expected
  defp aud_match?(_aud, _expected), do: false

  # ---------------------------------------------------------------------------
  # JWKS fetching / caching
  # ---------------------------------------------------------------------------

  defp maybe_refetch(state) do
    if refetch_allowed?(state) do
      fetch_keys(state)
    else
      {:error, :jwks_unavailable, state}
    end
  end

  defp refetch_allowed?(%{last_fetch_at: nil}), do: true
  defp refetch_allowed?(%{last_fetch_at: at}), do: now_ms() - at >= @refetch_cooldown_ms

  defp fetch_keys(state) do
    # Record the attempt regardless of outcome so a failing/unreachable JWKS
    # endpoint can't be hammered on every request.
    state = %{state | last_fetch_at: now_ms()}
    url = certs_url()

    case Req.get(url, retry: :transient, receive_timeout: 5_000) do
      {:ok, %Req.Response{status: 200, body: %{"keys" => keys}}} when is_list(keys) ->
        {:ok, %{state | keys: to_key_map(keys)}}

      {:ok, %Req.Response{status: status}} ->
        Logger.error("[CloudflareAccess] JWKS fetch failed with HTTP #{status} (#{url})")
        {:error, :jwks_fetch_failed, state}

      {:error, reason} ->
        Logger.error("[CloudflareAccess] JWKS fetch error: #{inspect(reason)} (#{url})")
        {:error, :jwks_fetch_error, state}
    end
  end

  defp certs_url do
    (config()[:team_domain] |> String.trim_trailing("/")) <> @certs_path
  end

  defp to_key_map(jwk_maps) do
    Map.new(jwk_maps, fn %{"kid" => kid} = map -> {kid, JOSE.JWK.from_map(map)} end)
  end

  defp now_ms, do: System.monotonic_time(:millisecond)

  defp config, do: Application.get_env(:sequin, __MODULE__, [])
end
