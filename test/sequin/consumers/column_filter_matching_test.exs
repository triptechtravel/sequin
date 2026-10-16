defmodule Sequin.Consumers.ColumnFilterMatchingTest do
  use Sequin.DataCase, async: true

  alias Sequin.Consumers
  alias Sequin.Factory.ReplicationFactory

  @table_oid 123

  defp pipeline(operator, value) do
    column_filter = ReplicationFactory.column_filter(column_attnum: 1, operator: operator, value: value)
    source_table = ReplicationFactory.source_table(oid: @table_oid, column_filters: [column_filter])
    ReplicationFactory.insert_wal_pipeline!(source_tables: [source_table])
  end

  defp message(value, action) do
    message = ReplicationFactory.postgres_message(action: action, table_oid: @table_oid)
    field = ReplicationFactory.field(column_attnum: 1, value: value)

    case action do
      :delete -> %{message | old_fields: [field]}
      _ -> %{message | fields: [field]}
    end
  end

  defp matches?(operator, filter_value, field_value, action \\ :insert) do
    Consumers.matches_message?(pipeline(operator, filter_value), message(field_value, action))
  end

  describe "datetime filters" do
    @after_2024 %{__type__: :datetime, value: ~U[2024-01-01 00:00:00Z]}

    test "never match, and don't raise on, a null value" do
      for op <- [:==, :>, :<, :>=, :<=] do
        refute matches?(op, @after_2024, nil)
        refute matches?(op, @after_2024, nil, :delete)
      end
    end

    test "order infinite timestamps around every finite one" do
      assert matches?(:>, @after_2024, "infinity")
      refute matches?(:<, @after_2024, "infinity")
      assert matches?(:<, @after_2024, "-infinity")
      refute matches?(:>, @after_2024, "-infinity")
    end

    test "compare naive and zoned timestamps and dates" do
      assert matches?(:>, @after_2024, ~N[2024-06-01 00:00:00])
      assert matches?(:>, @after_2024, ~U[2024-06-01 00:00:00Z])
      assert matches?(:>, @after_2024, ~D[2024-06-01])
      refute matches?(:>, @after_2024, ~D[2023-06-01])
    end
  end

  describe "number filters" do
    @hundred %{__type__: :number, value: 100}

    test "compare numerically against numeric (Decimal) columns" do
      assert matches?(:>, @hundred, Decimal.new("150.5"))
      refute matches?(:>, @hundred, Decimal.new("50"))
      assert matches?(:<, @hundred, Decimal.new("50"))
      assert matches?(:==, @hundred, Decimal.new("100.00"))
      assert matches?(:<=, @hundred, Decimal.new("100"))
    end

    test "null values satisfy no ordering" do
      for op <- [:==, :>, :<, :>=, :<=] do
        refute matches?(op, @hundred, nil)
      end

      # Unchanged: null is "not equal" to a number
      assert matches?(:!=, @hundred, nil)
    end

    test "keep comparing plain numbers" do
      assert matches?(:>, @hundred, 101)
      refute matches?(:>, @hundred, 99.5)
      assert matches?(:>=, @hundred, 100)
    end
  end
end
