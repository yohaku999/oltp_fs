# 20260522-063429

## Problem
This query timeouts after 30 sec.
``` sql
SELECT COUNT(DISTINCT (S_I_ID)) AS STOCK_COUNT FROM order_line, stock WHERE OL_W_ID = 1 AND OL_D_ID = 10 AND OL_O_ID < 3002 AND OL_O_ID >= 2982 AND S_W_ID = 1 AND S_I_ID = OL_I_ID AND S_QUANTITY < 10
```

## Observation
``` log
operator=SeqScanOperator event=close input_rows=0 output_rows=300069
operator=SeqScanOperator event=close input_rows=0 output_rows=100000
```
Currently naive loop join loops 300069 * 100000 times before reflecting available filtering.

## Next Step
This is primarily a problem regarding computational complexity and we may be able to solve this problem by implementing basic join algorithms like index join, hash join, sort merge join.
For another way, we can decrease the number of input_rows to join operator by filtering tuples beforehand. 
We are planning to implement all the join algorithms in the end, but since this selection pushdown seems to contribute more to decrease computational complexity, we decide to follow the idea of selection pushdown first.
### Scope
- add featrues to execute filtering inside scan/index/heap fetch before join.
- We still won't implement actual query planner yet. Keep with heuristics.
### Result
at 20260524-074358
epapesed_ms : 30100872 -> 968404
operator=LoopJoinOperator event=close input_rows=7941 output_rows=1668600 sources=2 child0_rows=216 child1_rows=7725