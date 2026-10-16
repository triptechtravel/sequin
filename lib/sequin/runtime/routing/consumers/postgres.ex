defmodule Sequin.Runtime.Routing.Consumers.Postgres do
  @moduledoc false
  use Sequin.Runtime.Routing.RoutedConsumer

  alias Sequin.Consumers.SinkConsumer

  @primary_key false
  @derive {Jason.Encoder, only: [:action, :table_name]}
  typed_embedded_schema do
    field :action, Ecto.Enum, values: [:upsert, :delete]
    field :table_name, :string
  end

  def changeset(struct, params) do
    allowed_keys = [:action, :table_name]

    struct
    |> cast(params, allowed_keys, empty_values: [])
    |> Routing.Helpers.validate_no_extra_keys(params, allowed_keys)
    |> validate_required([:action, :table_name])
    |> validate_inclusion(:action, [:upsert, :delete])
    |> validate_change(:table_name, fn :table_name, table_name ->
      case Sequin.Consumers.PostgresSink.parse_table_name(table_name) do
        {:ok, _} -> []
        :error -> [table_name: "must be a table name, optionally schema-qualified, like public.users"]
      end
    end)
  end

  # A routing function's result is merged over this, so rows go to a table with the same name as
  # their source table unless the function says otherwise
  def route(action, _record, _changes, metadata) do
    postgres_action =
      case action do
        "delete" -> :delete
        _insert_update_or_read -> :upsert
      end

    %{action: postgres_action, table_name: "#{metadata.table_schema}.#{metadata.table_name}"}
  end

  # Static routing sends every row to the sink's table; dynamic routing without a routing
  # function keeps the default
  def route_consumer(%SinkConsumer{sink: %{table_name: nil}}), do: %{}
  def route_consumer(%SinkConsumer{sink: sink}), do: %{table_name: sink.table_name}
end
