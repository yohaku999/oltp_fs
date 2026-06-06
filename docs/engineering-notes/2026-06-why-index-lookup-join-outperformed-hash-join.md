# Why Index Lookup Join Outperformed Hash Join

## Summary

This note records how my RDBMS implementation exposed the relationship between join strategy selection and available access paths.

I compared three strategies on a TPC-C StockLevel-style query:

| Strategy | Time | Role |
|---|---:|---|
| Nested Loop Join | 547ms | Baseline |
| Hash Join | 206ms | Removes naive `N * M` comparison |
| Index Nested Loop Join | 12ms | Uses the existing `stock(s_w_id, s_i_id)` index |

Hash Join improved over Nested Loop Join as expected. The notable result was that Index Nested Loop Join was still much faster than Hash Join.

The operator row logs suggest that the difference came less from the join algorithm alone and more from the access path: Hash Join had to process a broad `stock` range, while Index Nested Loop Join could use indexed point lookups through `stock(s_w_id, s_i_id)`.

## Query

```sql
SELECT COUNT(DISTINCT (S_I_ID)) AS STOCK_COUNT
FROM order_line, stock
WHERE OL_W_ID = 1
  AND OL_D_ID = 7
  AND OL_O_ID < 3002
  AND OL_O_ID >= 2982
  AND S_W_ID = 1
  AND S_I_ID = OL_I_ID
  AND S_QUANTITY < 14;
```

## Available Indexes

`order_line` primary key:

```text
(ol_w_id, ol_d_id, ol_o_id, ol_number)
```

Usable for:

```sql
OL_W_ID = 1
AND OL_D_ID = 7
AND OL_O_ID >= 2982
AND OL_O_ID < 3002
```

`stock` primary key:

```text
(s_w_id, s_i_id)
```

Usable for:

```sql
S_W_ID = 1
AND S_I_ID = OL_I_ID
```

## Why This Case Matters

Nested Loop Join to Hash Join is a textbook improvement. Hash Join avoids the naive `N * M` comparison pattern by building a hash table on one side and probing it from the other side.

But this experiment showed that avoiding `N * M` comparisons is not enough. A join strategy also determines how much data must be read and materialized before the join can happen.

Hash Join reduced comparison cost, but it still had to scan and materialize rows in order to build the temporary hash table. An existing index can sometimes avoid that input construction step: instead of first building a table-like structure for the join, Index Nested Loop Join can use the index to reach only the matching inner rows.

That is why this query is a useful case study for strategy selection:

```text
Hash Join:
  read a broad stock range
  materialize and filter stock rows
  build/probe a temporary hash table

Index Nested Loop Join:
  use filtered outer rows
  perform point lookups on stock(s_w_id, s_i_id)
```

## Supporting Operator Row Logs

Hash Join:

```text
IndexScanOperator: input_rows=1 -> output_rows=100000 (predicates=2)
HeapFetchOperator: input_rows=100000 -> output_rows=3156
IndexScanOperator: input_rows=1 -> output_rows=172 (predicates=4)
HeapFetchOperator: input_rows=172 -> output_rows=172
HashJoinOperator: input_rows=3328 -> output_rows=3 (hash_table_keys=3156)
FilterOperator: input_rows=3 -> output_rows=3
AggregateOperator: input_rows=3 -> output_rows=1
```

Index Nested Loop Join:

```text
IndexScanOperator: input_rows=1 -> output_rows=226 (predicates=4)
HeapFetchOperator: input_rows=226 -> output_rows=226
IndexLookupJoinOperator: input_rows=452 -> output_rows=11 (index_lookups=226)
FilterOperator: input_rows=11 -> output_rows=11
AggregateOperator: input_rows=11 -> output_rows=1
```

The operator row logs were collected from runs with the same query shape, but the literal values differed slightly from the timing benchmark. Therefore, I use these logs only as supporting evidence for the access-path pattern, not as an exact explanation of the measured elapsed-time difference.

Even with this caveat, the important access-path pattern is visible: Hash Join processed thousands of materialized `stock` rows, while Index Nested Loop Join performed a small number of point lookups.

The Hash Join implementation also built the hash table on the larger side in this run:

```text
stock side:      3156 rows
order_line side: 172 rows
hash_table_keys: 3156
```

For Hash Join alone, it would have been better to build on the smaller `order_line` side. However, that does not explain the whole 10x+ difference. Even if the build/probe sides were swapped, Hash Join would still need to read and filter the broad `stock` side.

This is the strongest explanation supported by the operator row logs:

```text
Hash Join:
  stock index output: 100000
  stock heap fetch input: 100000
  hash table rows: 3156

Index Nested Loop Join:
  outer rows: 226
  stock index lookups: 226
```

So the win here was not only about avoiding hash table build cost. The bigger win was avoiding a broad `stock` scan and replacing it with a small number of indexed point lookups.

## Takeaways

- I confirmed in my own implementation that Hash Join is not automatically the best choice once a useful index access path exists.
- The availability of an index access path can be as important as the join algorithm itself.
- The Hash Join implementation should prefer building the hash table on the smaller side, but that optimization alone would not remove the broad `stock` scan.
- Hash Join can still be the better strategy when the outer side is large, the join key is not selective, the inner side has no useful index, or both sides must be scanned anyway.

## Future Work

- Collect more stable metrics using the exact same query literals, so that elapsed time and operator row logs can be compared directly.
- Improve Hash Join by choosing the smaller input as the build side.
- Add a simple rule-based optimizer that prefers Index Nested Loop Join when the outer side is small and the inner side has a usable index on the join key.
