defmodule SequinWeb.Plugs.CloudflareAccess do
  @moduledoc """
  Authenticates browser requests from a verified Cloudflare Access assertion.

  When enabled, this plug runs in the `:browser` pipeline *before*
  `fetch_current_user/2`. On the first request of a session it verifies the
  `Cf-Access-Jwt-Assertion` header (see `Sequin.CloudflareAccess`), provisions /
  looks up the matching user, and stores a normal Sequin session token — so the
  user is signed in transparently and never sees Sequin's own login screen.

  It is a no-op when Cloudflare Access is disabled, or when the request already
  carries a session token (the fast path for every request after the first).
  """
  import Plug.Conn

  require Logger

  alias Sequin.Accounts
  alias Sequin.CloudflareAccess
  alias SequinWeb.UserAuth

  @header "cf-access-jwt-assertion"

  def init(opts), do: opts

  def call(conn, _opts) do
    cond do
      not CloudflareAccess.enabled?() -> conn
      get_session(conn, :user_token) -> conn
      true -> authenticate(conn)
    end
  end

  defp authenticate(conn) do
    with [token | _] <- get_req_header(conn, @header),
         {:ok, claims} <- CloudflareAccess.verify_token(token),
         {:ok, user} <- Accounts.find_or_create_cloudflare_access_user(claims) do
      UserAuth.put_user_in_session(conn, user)
    else
      # No assertion present — let the request continue unauthenticated; the
      # existing auth guards will handle it (this also covers health checks).
      [] ->
        conn

      {:error, reason} ->
        Logger.warning("[CloudflareAccess] rejected assertion: #{inspect(reason)}")
        conn
    end
  end
end
