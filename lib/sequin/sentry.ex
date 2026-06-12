defmodule Sequin.Sentry do
  @moduledoc false
  def init do
    env = Application.get_env(:sequin, :env)

    cond do
      System.get_env("CRASH_REPORTING_DISABLED") in ~w(true 1) ->
        Sentry.put_config(:dsn, nil)

      env == :prod ->
        # Upstream raised when no DSN was baked in at build time, assuming
        # their CI always supplied one. This fork builds DSN-less by default
        # (config/prod.exs normalizes "" to nil) — treat that as disabled.
        if is_nil(Application.get_env(:sentry, :dsn)) do
          Sentry.put_config(:dsn, nil)
        else
          :logger.add_handler(:sentry_handler, Sentry.LoggerHandler, %{})
        end

      true ->
        :logger.add_handler(:sentry_handler, Sentry.LoggerHandler, %{})
    end
  end
end
