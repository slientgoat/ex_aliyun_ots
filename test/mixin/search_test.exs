defmodule ExAliyunOts.MixinTest.Search do
  
  use ExUnit.Case

  @instance_key EDCEXTestInstance

  use ExAliyunOts,
    instance: @instance_key

  require Logger
  alias ExAliyunOts.Client
  alias ExAliyunOts.Var.Search

  alias ExAliyunOtsTest.Support.Search, as: TestSupportSearch

  @table "test_search"

  @indexes ["test_search_index", "test_search_index2"]

  setup_all do
    Application.ensure_all_started(:ex_aliyun_ots)

    TestSupportSearch.initialize(@instance_key, @table, @indexes)

    on_exit(fn ->
      TestSupportSearch.clean(@instance_key, @table, @indexes)
    end)

    :ok
  end

  test "list search index" do
    {:ok, response} = list_search_index(@table)
    assert length(response.indices) == 2
  end

  test "describe search index" do
    index_name = "test_search_index2"
    {:ok, response} = describe_search_index(@table, index_name)
    schema = response.schema
    field_schemas = schema.field_schemas
    Enum.map(field_schemas, fn(field_schema) ->
      assert field_schema.field_type ==  FieldType.nested
      Enum.with_index(field_schema.field_schemas)
      |> Enum.map(fn({sub_field_schema, index}) ->
        cond do
          index == 0 ->
            assert sub_field_schema.field_name == "header"
            assert sub_field_schema.field_type == FieldType.keyword
          index == 1 ->
            assert sub_field_schema.field_name == "body"
            assert sub_field_schema.field_type == FieldType.keyword
        end
      end)
    end)
  end

  test "match query" do
    index_name = "test_search_index"
    {:ok, response} = 
      search @table, index_name,
        columns_to_get: {ColumnReturnType.specified, ["class", "name"]},
        #columns_to_get: ColumnReturnType.none,
        #columns_to_get: ColumnReturnType.all,
        #columns_to_get: ["class"],
        search_query: [
          query: [
            type: QueryType.match,
            field_name: "age",
            text: "28"
          ],
          limit: 1
        ]
    assert response.total_hits == 2
    [{[{_pk_key, pk_value}], attrs}] = response.rows
    assert pk_value == "a2"
    assert length(attrs) == 2
  end

  test "term query" do
    index_name = "test_search_index"
    {:ok, response} =
      search @table, index_name,
        search_query: [
          query: [
            type: QueryType.term,
            field_name: "age",
            term: 28
          ],
        ]
    assert response.total_hits == 2
    assert length(response.rows) == 2
  end

  test "terms query with sort" do
    index_name = "test_search_index"
    {:ok, response} =
      search @table, index_name,
        search_query: [
          query: [
            type: QueryType.terms,
            field_name: "age",
            terms: [22, 26, 27]
          ],
          sort: [
            [type: SortType.field, field_name: "age", order: SortOrder.asc],
            [type: SortType.field, field_name: "name", order: SortOrder.asc]
          ]
        ]
    assert response.total_hits == 3
    assert length(response.rows) == 3
  end

  test "prefix query" do
    index_name = "test_search_index"
    {:ok, response} =
      search @table, index_name,
        search_query: [
          query: [
            type: QueryType.prefix,
            field_name: "name",
            prefix: "n"
          ],
          sort: [
            [type: SortType.field, field_name: "age", order: SortOrder.asc],
            [type: SortType.field, field_name: "name", order: SortOrder.asc]
          ]
        ],
        columns_to_get: ["age", "name"]
    assert response.total_hits == 8
    assert length(response.rows) == 8
  end

  test "wildcard query" do
    index_name = "test_search_index"
    {:ok, response} =
      search @table, index_name,
        search_query: [
          query: [
            type: QueryType.wildcard,
            field_name: "name",
            value: "n*"
          ],
          sort: [
            [type: SortType.field, field_name: "age", order: SortOrder.asc],
            [type: SortType.field, field_name: "name", order: SortOrder.asc]
          ]
        ],
        columns_to_get: ColumnReturnType.all
    assert response.total_hits == 8
    assert length(response.rows) == 8
  end

  test "range query" do
    index_name = "test_search_index"
    {:ok, response} =
      search @table, index_name,
        search_query: [
          query: [
            type: QueryType.range,
            field_name: "score",
            from: 60,
            to: 80,
            include_upper: false, # `include_upper` as true and `include_lower` as true by default
            include_lower: false 
          ],
          sort: [
            [type: SortType.field, field_name: "age", order: SortOrder.desc],
            [type: SortType.field, field_name: "name", order: SortOrder.asc]
          ]
        ]
    assert response.total_hits == 2
    primary_keys = 
      Enum.map(response.rows, fn(row) ->
        {[{_pk_key, pk_value}], _attrs} = row
        pk_value
      end)
    assert primary_keys == ["a3", "a6"]
  end

  test "bool query with must/must_not" do
    index_name = "test_search_index"
    {:ok, response} =
      search @table, index_name,
        search_query: [
          query: [
            type: QueryType.bool,
            must: [
              [type: QueryType.range, field_name: "age", from: 20, to: 32],
            ],
            must_not: [
              [type: QueryType.term, field_name: "age", term: 28]
            ],
          ],
          sort: [
            [type: SortType.field, field_name: "age", order: SortOrder.desc],
            [type: SortType.field, field_name: "name", order: SortOrder.asc]
          ]
        ]

    assert response.total_hits == 6
    assert length(response.rows) == 6

    attr_ages = 
      Enum.map(response.rows, fn({[{_pk_key, _pk_value}], attrs}) ->
        {"age", age, _ts} = List.first(attrs)
        age
      end)
    assert Enum.sort(attr_ages, &(&1 >= &2)) == attr_ages
    assert 28 not in attr_ages
  end

  test "bool query with should" do
    index_name = "test_search_index"
    {:ok, response} =
      search @table, index_name,
        search_query: [
          query: [
            type: QueryType.bool,
            should: [
              [type: QueryType.range, field_name: "age", from: 20, to: 32],
              [type: QueryType.term, field_name: "score", term: 66.78]
            ],
            minimum_should_match: 2
          ],
          sort: [
            [type: SortType.field, field_name: "age", order: SortOrder.desc],
            [type: SortType.field, field_name: "name", order: SortOrder.asc]
          ]
        ]
    assert response.total_hits == 1
    assert length(response.rows) == 1
    [{[{_pk_key, pk_value}], _attrs}] = response.rows
    assert pk_value == "a3"
  end

  test "nested query" do
    index_name = "test_search_index2"
    {:ok, response} =
      search @table, index_name,
        search_query: [
          query: [
            type: QueryType.nested,
            path: "content",
            query: [
              type: QueryType.term, field_name: "content.header", term: "header1"
            ]
          ]
        ]
    assert response.total_hits == 1
    assert length(response.rows) == 1

    [{[{"partition_key", id}], [{"content", value, _ts}]}] = response.rows

    assert id == "a9"
    assert value == "[{\"body\":\"body1\",\"header\":\"header1\"}]"
  end

  test "delete search index" do
    index_name = "tmp_search_index1"
    var_request =
      %Search.CreateSearchIndexRequest{
        table_name: @table,
        index_name: index_name,
        index_schema: %Search.IndexSchema{
          field_schemas: [
            %Search.FieldSchema{
              field_name: "name",
            },
          ]
        }
      }
    {result, _response} = Client.create_search_index(@instance_key, var_request)
    assert result == :ok

    Process.sleep(3_000)
    {result, _response} = delete_search_index(@table, index_name)
    assert result == :ok
  end
end
