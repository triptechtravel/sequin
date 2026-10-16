defmodule Sequin.Runtime.KeysetCursor do
  @moduledoc false
  alias Sequin.Databases.PostgresDatabaseTable, as: Table
  alias Sequin.Postgres

  @type cursor :: map()

  @spec attnums_to_names(Table.t(), map()) :: map()
  def attnums_to_names(%Table{} = table, cursor) do
    Map.new(cursor, fn {attnum, value} ->
      name =
        table.columns
        |> Sequin.Enum.find!(fn column -> column.attnum == attnum end)
        |> Map.fetch!(:name)

      {name, value}
    end)
  end

  @spec cursor_columns(Table.t()) :: [Table.Column.t()]
  def cursor_columns(%Table{sort_column_attnum: nil} = table) do
    # When sort_column_attnum is nil, just use primary keys in order of attnum
    table.columns |> Enum.filter(& &1.is_pk?) |> Enum.sort_by(& &1.attnum)
  end

  @spec cursor_columns(Table.t()) :: [Table.Column.t()]
  def cursor_columns(%Table{sort_column_attnum: sort_column_attnum} = table) do
    sort_column = Sequin.Enum.find!(table.columns, fn column -> column.attnum == sort_column_attnum end)
    sorted_pks = table.columns |> Enum.filter(& &1.is_pk?) |> Enum.sort_by(& &1.attnum)

    if sort_column.is_pk? do
      [sort_column | Enum.filter(sorted_pks, &(&1.attnum != sort_column_attnum))]
    else
      [sort_column | sorted_pks]
    end
  end

  @doc """
  Given a table and a minimum sort column value, return a cursor with the minimum values for all columns
  """
  @spec min_cursor(%Table{}, any()) :: map()
  def min_cursor(%Table{sort_column_attnum: nil} = table, nil) do
    # When there's no sort column, we just use minimum values for all primary keys
    pk_columns = cursor_columns(table)

    Map.new(pk_columns, fn column ->
      {column.attnum, min_for_type(column.type)}
    end)
  end

  @spec min_cursor(%Table{}, any()) :: map()
  def min_cursor(%Table{} = table, sort_col_value) do
    [sort_column | pk_columns] = cursor_columns(table)

    pk_cursor =
      Map.new(pk_columns, fn column ->
        {column.attnum, min_for_type(column.type)}
      end)

    Map.put(pk_cursor, sort_column.attnum, sort_col_value)
  end

  @doc """
  Given a table, return a cursor with the minimum values for all columns
  """

  def min_cursor(%Table{sort_column_attnum: nil} = table) do
    # When there's no sort column, we just use minimum values for all primary keys
    min_cursor(table, nil)
  end

  def min_cursor(%Table{} = table) do
    [sort_column | _pk_columns] = cursor_columns(table)
    sort_col_value = min_for_type(sort_column.type)
    min_cursor(table, sort_col_value)
  end

  # nil means "unbounded": no value of any type is guessed as the minimum (0 isn't one for integers,
  # and many types have none that can be passed as a parameter). where_sql/3 and
  # casted_cursor_values/2 compare only the bounded prefix of a cursor.
  def min_for_type(_type), do: nil

  @spec where_sql(Table.t(), String.t(), map() | nil) :: String.t()
  def where_sql(%Table{} = table, operator, cursor \\ nil) do
    columns = bounded_columns(table, cursor)

    if columns == [] do
      "true"
    else
      quoted_columns = columns |> quoted_column_names() |> Enum.join(", ")
      rhs = "(#{Enum.map_join(columns, ", ", &"(?::text)::#{&1.type}")})"

      # (a, b) > (x, <unbounded>) is a >= x, as nothing sorts before an unbounded b
      operator =
        if length(columns) < length(cursor_columns(table)),
          do: Map.get(%{">" => ">=", "<" => "<="}, operator, operator),
          else: operator

      "(#{quoted_columns}) #{operator} #{rhs}"
    end
  end

  # The cursor columns up to the first unbounded (nil) value
  defp bounded_columns(%Table{} = table, nil), do: cursor_columns(table)

  defp bounded_columns(%Table{} = table, cursor) do
    table |> cursor_columns() |> Enum.take_while(&(not is_nil(Map.get(cursor, &1.attnum))))
  end

  @spec order_by_sql(Table.t(), String.t()) :: String.t()
  def order_by_sql(%Table{} = table, direction \\ "asc") do
    # Qualified, as a bare name in ORDER BY refers to a select-list alias first, and backfills
    # select some columns as text under their own name (see Postgres.safe_select_columns/1).
    # Ordering by that text instead of the column disagrees with where_sql/3, skipping rows.
    qualified_table = Postgres.quote_name(table.schema, table.name)

    table
    |> cursor_columns()
    |> quoted_column_names()
    |> Enum.map_join(", ", &"#{qualified_table}.#{&1} #{direction}")
  end

  @doc """
  Cursor values as Postgres text, in the order of where_sql/3's placeholders, which cast each
  back to its column's type. Text round-trips every type (money, inet, composites, infinite
  dates, ...), including the strings that cursors persisted as JSON hold.
  """
  @spec casted_cursor_values(Table.t(), map()) :: [String.t()]
  def casted_cursor_values(%Table{} = table, cursor) do
    table
    |> bounded_columns(cursor)
    |> Enum.map(&cursor_text(Map.fetch!(cursor, &1.attnum)))
  end

  defp cursor_text(value) when is_binary(value), do: value
  defp cursor_text(%NaiveDateTime{} = value), do: NaiveDateTime.to_iso8601(value)
  defp cursor_text(%DateTime{} = value), do: DateTime.to_iso8601(value)
  defp cursor_text(%Date{} = value), do: Date.to_iso8601(value)
  defp cursor_text(%Time{} = value), do: Time.to_iso8601(value)
  defp cursor_text(%Decimal{} = value), do: Decimal.to_string(value, :normal)
  defp cursor_text(value), do: to_string(value)

  @spec cursor_from_row(Table.t(), map()) :: map()
  def cursor_from_row(%Table{} = table, row) do
    cursor_columns = cursor_columns(table)

    Map.new(cursor_columns, fn
      %Table.Column{type: "uuid"} = column ->
        value = Map.fetch!(row, column.name)
        {column.attnum, value}

      %Table.Column{} = column ->
        value = Map.fetch!(row, column.name)

        {column.attnum, value}
    end)
  end

  defp quoted_column_names(cursor_columns) do
    Enum.map(cursor_columns, fn %Table.Column{} = col -> Postgres.quote_name(col.name) end)
  end
end
