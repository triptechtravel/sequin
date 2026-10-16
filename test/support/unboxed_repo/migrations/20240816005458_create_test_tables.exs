defmodule Sequin.Test.UnboxedRepo.Migrations.CreateTestTables do
  @moduledoc false
  use Ecto.Migration

  alias Sequin.Constants

  def my_version, do: 3

  def check_version!(repo) do
    if my_version() != get_version(repo) do
      IO.puts(
        :stderr,
        [
          IO.ANSI.red(),
          "UnboxedRepo version has been updated!",
          IO.ANSI.reset(),
          "\n",
          IO.ANSI.bright(),
          "Please run the following command to re-create:",
          IO.ANSI.reset(),
          "\n\n",
          "MIX_ENV=test mix ecto.reset\n"
        ]
      )

      System.halt(1)
    end
  end

  defp get_version(repo) do
    case repo.query("select my_version from my_version_info") do
      {:error, _} -> nil
      {:ok, %Postgrex.Result{rows: [[version]]}} -> version
    end
  end

  def change do
    execute "create table my_version_info as select #{my_version()} as my_version"

    create table(:Characters) do
      add :name, :text
      add :house, :text
      add :planet, :text
      add :is_active, :boolean
      add :tags, {:array, :text}
      add :metadata, :map

      timestamps(type: :naive_datetime_usec)
    end

    create table(:characters_ident_full) do
      add :name, :text
      add :house, :text
      add :planet, :text
      add :is_active, :boolean
      add :tags, {:array, :text}

      timestamps(type: :naive_datetime_usec)
    end

    execute "alter table characters_ident_full replica identity full"

    # New table with multiple primary keys of different types
    create table(:characters_multi_pk, primary_key: false) do
      add :id_integer, :serial, primary_key: true
      add :id_string, :string, primary_key: true
      add :id_uuid, :uuid, primary_key: true
      add :name, :text
      add :house, :text

      timestamps(type: :naive_datetime_usec)
    end

    execute "CREATE EXTENSION IF NOT EXISTS pg_trgm"
    execute "CREATE EXTENSION IF NOT EXISTS vector"
    execute "CREATE TYPE character_status AS ENUM ('active', 'inactive', 'retired')"

    # Add a domain type for positive integers
    execute "CREATE DOMAIN positive_int AS integer CHECK (VALUE > 0)"

    # New table with all possible column types
    create table(:characters_detailed) do
      add :name, :string
      add :status, :character_status
      # Using our domain type
      add :power_level, :positive_int
      add :age, :integer
      add :height, :float
      add :is_hero, :boolean
      add :biography, :text
      add :biography_tsv, :tsvector, generated: "ALWAYS AS (to_tsvector('english', coalesce(biography, ''))) STORED"
      add :birth_date, :date
      add :last_seen, :time
      add :powers, {:array, :string}
      add :metadata, :map
      add :rating, :decimal
      add :avatar, :binary
      add :house_id, :uuid
      add :net_worth, :money
      add :email, :citext
      add :binary_data, :bytea
      add :related_houses, {:array, :uuid}
      add :active_period, :daterange
      add :embedding, :vector, size: 3

      timestamps(type: :naive_datetime_usec)
    end

    # Older version without transaction_annotations
    create table(:test_event_logs_v0, primary_key: false) do
      add :id, :serial, primary_key: true
      add :seq, :bigint, null: false
      add :source_database_id, :uuid, null: false
      add :source_table_oid, :bigint, null: false
      add :source_table_schema, :text, null: false
      add :source_table_name, :text, null: false
      add :record_pk, :text, null: false
      add :record, :map, null: false
      add :changes, :map
      add :action, :string, null: false
      add :committed_at, :naive_datetime_usec, null: false
      add :inserted_at, :naive_datetime_usec, null: false, default: fragment("NOW()")
    end

    create unique_index(:test_event_logs_v0, [:source_database_id, :committed_at, :seq, :record_pk])

    create index(:test_event_logs_v0, [:seq])
    create index(:test_event_logs_v0, [:source_table_oid])
    create index(:test_event_logs_v0, [:committed_at])

    create table(:test_event_logs, primary_key: false) do
      add :id, :serial, primary_key: true
      add :seq, :bigint, null: false
      add :source_database_id, :uuid, null: false
      add :source_table_oid, :bigint, null: false
      add :source_table_schema, :text, null: false
      add :source_table_name, :text, null: false
      add :record_pk, :text, null: false
      add :record, :map, null: false
      add :changes, :map
      add :action, :string, null: false
      add :transaction_annotations, :map
      add :committed_at, :naive_datetime_usec, null: false
      add :inserted_at, :naive_datetime_usec, null: false, default: fragment("NOW()")
    end

    create unique_index(:test_event_logs, [:source_database_id, :committed_at, :seq, :record_pk])
    create index(:test_event_logs, [:seq])
    create index(:test_event_logs, [:source_table_oid])
    create index(:test_event_logs, [:committed_at])

    # Simulate a pg_partman partitioned table
    create table(:test_event_logs_partitioned, primary_key: false, options: "PARTITION BY RANGE (committed_at)") do
      add :id, :bigserial, null: false, primary_key: true
      add :seq, :bigint, null: false
      add :source_database_id, :uuid, null: false
      add :source_table_oid, :bigint, null: false
      add :source_table_schema, :text, null: false
      add :source_table_name, :text, null: false
      add :record_pk, :text, null: false
      add :record, :map, null: false
      add :changes, :map
      add :action, :string, null: false
      add :committed_at, :naive_datetime_usec, null: false, primary_key: true
      add :inserted_at, :naive_datetime_usec, null: false, default: fragment("NOW()")
    end

    execute "CREATE TABLE test_event_logs_partitioned_default PARTITION OF test_event_logs_partitioned DEFAULT"

    create unique_index(:test_event_logs_partitioned, [:source_database_id, :committed_at, :seq, :record_pk])
    create index(:test_event_logs_partitioned, [:seq])
    create index(:test_event_logs_partitioned, [:source_table_oid])
    create index(:test_event_logs_partitioned, [:committed_at])

    execute "COMMENT ON TABLE test_event_logs_partitioned IS '$sequin-events$'"

    execute Sequin.Postgres.logical_messages_table_ddl(),
            "drop table if exists #{Constants.logical_messages_table_name()}"

    # One column per Postgres type Sequin may meet, for asserting that a backfilled row and the
    # same row streamed through logical replication produce identical records.
    execute "CREATE EXTENSION IF NOT EXISTS citext"
    execute "CREATE EXTENSION IF NOT EXISTS hstore"

    execute "CREATE TYPE money_with_currency AS (currency_code varchar, amount numeric)",
            "DROP TYPE money_with_currency"

    execute "CREATE TYPE postal_address AS (street text, city text, unit int)", "DROP TYPE postal_address"

    execute """
            CREATE TABLE all_column_types (
              id serial PRIMARY KEY,
              c_bool boolean,
              c_int2 smallint,
              c_int4 integer,
              c_int8 bigint,
              c_numeric numeric,
              c_numeric_scaled numeric(10, 4),
              c_numeric_nan numeric,
              c_float4 real,
              c_float8 double precision,
              c_float8_special double precision,
              c_money money,
              c_char char(5),
              c_varchar varchar(20),
              c_text text,
              c_name name,
              c_qchar "char",
              c_citext citext,
              c_bytea bytea,
              c_date date,
              c_date_infinity date,
              c_time time,
              c_timetz timetz,
              c_timestamp timestamp,
              c_timestamp_infinity timestamp,
              c_timestamptz timestamptz,
              c_interval interval,
              c_uuid uuid,
              c_json json,
              c_jsonb jsonb,
              c_inet inet,
              c_cidr cidr,
              c_macaddr macaddr,
              c_macaddr8 macaddr8,
              c_bit bit(4),
              c_varbit varbit,
              c_point point,
              c_line line,
              c_lseg lseg,
              c_box box,
              c_path path,
              c_polygon polygon,
              c_circle circle,
              c_tsvector tsvector,
              c_tsquery tsquery,
              c_xml xml,
              c_pg_lsn pg_lsn,
              c_oid oid,
              c_regclass regclass,
              c_int4range int4range,
              c_int8range int8range,
              c_numrange numrange,
              c_tsrange tsrange,
              c_tstzrange tstzrange,
              c_daterange daterange,
              c_empty_range int4range,
              c_unbounded_range int4range,
              c_int4multirange int4multirange,
              c_hstore hstore,
              c_vector vector(3),
              c_halfvec halfvec(3),
              c_sparsevec sparsevec(5),
              c_enum character_status,
              c_domain positive_int,
              c_composite money_with_currency,
              c_composite_tricky postal_address,
              c_bool_arr boolean[],
              c_int4_arr integer[],
              c_int4_2d_arr integer[][],
              c_int8_arr bigint[],
              c_numeric_arr numeric[],
              c_float8_arr double precision[],
              c_text_arr text[],
              c_varchar_arr varchar[],
              c_bytea_arr bytea[],
              c_date_arr date[],
              c_time_arr time[],
              c_timetz_arr timetz[],
              c_timestamp_arr timestamp[],
              c_timestamptz_arr timestamptz[],
              c_interval_arr interval[],
              c_uuid_arr uuid[],
              c_json_arr json[],
              c_jsonb_arr jsonb[],
              c_inet_arr inet[],
              c_money_arr money[],
              c_xml_arr xml[],
              c_int4range_arr int4range[],
              c_enum_arr character_status[],
              c_domain_arr positive_int[],
              c_composite_arr money_with_currency[],
              updated_at timestamp NOT NULL DEFAULT now()
            )
            """,
            "DROP TABLE all_column_types"

    # A primary key of types whose Postgres text differs from Elixir's to_string/1 of the decoded
    # value, for asserting backfill and replication agree on message identity
    execute """
            CREATE TABLE pk_types (
              pk_timestamp timestamp,
              pk_timestamptz timestamptz,
              pk_bool boolean,
              pk_numeric numeric,
              name text,
              updated_at timestamp NOT NULL DEFAULT now(),
              PRIMARY KEY (pk_timestamp, pk_timestamptz, pk_bool, pk_numeric)
            )
            """,
            "DROP TABLE pk_types"

    execute "create publication characters_publication for table \"Characters\", characters_ident_full, characters_multi_pk, characters_detailed, all_column_types, pk_types, test_event_logs_partitioned, #{Constants.logical_messages_table_name()}",
            "drop publication characters_publication"

    create table(:sequin_events) do
      add :seq, :bigint, null: false
      add :source_database_id, :uuid, null: false
      add :source_table_oid, :bigint, null: false
      add :source_table_schema, :text, null: false
      add :source_table_name, :text, null: false
      add :record_pk, :text, null: false
      add :record, :map, null: false
      add :changes, :map
      add :action, :text, null: false
      add :committed_at, :naive_datetime_usec, null: false
      add :inserted_at, :naive_datetime_usec, null: false, default: fragment("NOW()")
    end

    create unique_index(:sequin_events, [:source_database_id, :committed_at, :seq, :record_pk])

    # Not included in the publication
    create table(:my_non_published_table) do
    end
  end
end
