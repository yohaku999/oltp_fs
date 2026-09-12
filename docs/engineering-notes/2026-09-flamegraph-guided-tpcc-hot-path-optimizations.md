# Flamegraph-Guided TPC-C Hot-Path Optimizations

## Summary

A series of small optimizations was applied to TPC-C hot paths, such as caching
parsed SQL templates, removing redundant B-tree sorting, checking the metadata
cache before the filesystem, combining response writes, making frame-directory
lookup cheaper, and stopping B-tree leaf scans at the right boundary.

The measurements were cumulative and fluctuated between steps, but the overall
direction was positive. Throughput increased from 246.13 to 285.00 requests per
second (+15.8%), while average latency fell from 4.050 to 3.495 ms (-13.7%).

The flamegraphs showed smaller relative weights in the code paths targeted by
the changes. This is supporting context rather than proof of an absolute CPU
reduction. Together with the end-to-end direction, it suggests that the series
likely produced a modest improvement.

## Measurement scope

All measurements used the same BenchBase configuration:

- TPC-C scale factor 1
- one terminal
- 60-second warmup
- 300-second measurement interval
- regular arrival at an effectively unlimited target rate
- transaction weights `45,43,4,4,4`

The changes were applied cumulatively in the order shown. Each row therefore
contains its named change and every preceding change. These are not independent
applications to a common baseline.

| Latest change in cumulative code state | Throughput | Average | P95 |
| --- | ---: | ---: | ---: |
| Cache parsed SQL templates | 246.13 req/s | 4.050 ms | 11.301 ms |
| Remove internal-page sorting | 265.51 req/s | 3.755 ms | 9.331 ms |
| Check the metadata cache first | 249.35 req/s | 3.995 ms | 11.706 ms |
| Send a contiguous response frame | 275.82 req/s | 3.613 ms | 10.244 ms |
| Use hashed frame-directory lookup | 290.22 req/s | 3.434 ms | 9.429 ms |
| Stop leaf traversal at the right boundary | 285.00 req/s | 3.495 ms | 10.161 ms |

## Flamegraph observations

The global top-five ranking changed little. B-tree and buffer-pool work
remained dominant throughout the series. The table records the relative change
in the specific stack targeted by each optimization.

| Change | Target stack | Before | After | Relative reduction |
| --- | --- | ---: | ---: | ---: |
| Remove internal-page sorting | `InternalIndexPage::findChildPage()` | 18.61% | 3.79% | 79.6% |
| Check the metadata cache first | `TableMetadataStore::read()` | 5.88% | 0.73% | 87.6% |
| Send a contiguous response frame | `__send` | 18.18% | 13.56% | 25.4% |
| Use hashed frame-directory lookup | `FrameDirectory::findResidentFrame()` | 6.54% | 2.75% | 58.0% |
| Stop at the right boundary | `LeafIndexPage::findEntries()` | 8.76% | 5.99% | 31.6% |
| Stop at the right boundary | `BTreeCursor::isInsideBoundary()` | 2.26% | 1.27% | 43.8% |

These values are references, not measurements of the speedup contributed by
each change. They are relative inclusive weights, so they can change when
other parts of the workload become more or less expensive.

A stronger evaluation would apply every change independently to one baseline,
run each state multiple times, and compare throughput, latency, and absolute
CPU counters over the measurement interval. That work was intentionally
omitted here: these were small, mechanically motivated changes, and the cost
of isolating every contribution would exceed the value of the result. The
cumulative benchmark and flamegraphs are used only as a lightweight check that
the changes behaved in the expected direction overall.

## 1. Cache parsed SQL templates

The old request path rendered parameter values into SQL text and parsed the
result for every execution. Prepared statements repeat a small set of SQL
shapes, so reparsing a new string for every parameter set did not add useful
information.

The server now caches a parser by SQL template. Placeholders are converted to
PostgreSQL-style parameter references once, while request values are decoded
into `QueryParameters` and supplied later to the parser and binder. This is a
syntax-tree cache, not a physical-plan cache: binding and plan selection still
occur for each execution.

This is the first measured state in the series, so there is no adjacent
pre-change profile from which to isolate its effect.

## 2. Remove sorting from internal B-tree traversal

`InternalIndexPage::findChildPage()` previously copied every valid separator
cell into a vector and sorted that vector on every tree descent. B-tree page
slots are already maintained in key order. The new path scans valid cells in
page order and stops at the first key greater than the search key.

The target stack fell from 18.61% to 3.79%. Throughput also increased by 7.9%
and average latency fell by 7.3%. The implementation change, profile movement,
and end-to-end result all point in the same direction.

## 3. Check the table metadata cache before the filesystem

`TableMetadataStore::read()` used to call `filesystem::exists()` before
checking its in-memory cache. A metadata cache hit therefore still performed a
filesystem metadata lookup. Reordering the checks makes the common cached path
memory-only while preserving the existing miss behavior.

The target stack fell from 5.88% to 0.73%. Throughput declined in the adjacent
measurement, so an end-to-end gain cannot be assigned to this change. The
profile nevertheless shows that the unnecessary filesystem path was removed.

## 4. Send one response frame with one call

The response protocol consists of a four-byte length prefix followed by a JSON
payload. Even with `TCP_NODELAY`, the server issued one `send()` for the prefix
and another for the payload. `writeFrame()` now builds one contiguous frame
and passes it to `sendAll()` once.

This trades a small allocation and two memory copies for one fewer normal-case
socket call. The `__send` stack fell from 18.18% to 13.56%, throughput increased
by 10.6%, and average latency fell by 9.6%.

## 5. Make frame-directory lookup cheaper

`BufferPool::pinPage()` consults the frame directory on every page access. The
old path constructed a `(page_id, file_path)` pair for lookup, copied the file
path in several calls, and searched an ordered map. The revised path constructs
one `PageKey`, passes it by reference, uses average `O(1)` hash lookup, and
reserves enough buckets for the maximum frame count.

The inclusive `findResidentFrame()` stack fell from 6.54% to 2.75%, while its
self weight fell from 3.76% to 0.33%. Throughput increased by 5.2% and average
latency fell by 5.0%.

## 6. Stop leaf traversal at the right boundary

The leaf scan previously checked the right boundary once in the match
condition, checked it again in the fallback branch, and continued examining
later slots after crossing it. Since valid leaf cells are key ordered, no later
cell can return to the requested range.

The revised loop checks the right boundary first and breaks immediately,
checks the left boundary once, and only then constructs the RID and evaluates
the optional RID filter.

`LeafIndexPage::findEntries()` fell from 8.76% to 5.99%, and boundary checking
fell from 2.26% to 1.27%. End-to-end throughput declined by 1.8% in the adjacent
measurement, so the profile supports a local CPU improvement but not a
separately measurable TPC-C gain.

## Conclusion

The six changes should not be presented as six independently proven
performance wins. The benchmark points are cumulative, and two adjacent
measurements moved against the expected direction.

It is nevertheless reasonable to say that the series likely improved DBFS.
The observed endpoints show roughly 16% higher throughput and 14% lower
average latency, while the targeted stacks became less prominent in the
profiles. The exact contribution of each change is unknown, and the
flamegraph percentages should not be interpreted as speedups.

B-tree lookup and buffer-pool page access remained dominant. Further progress
will require changing those underlying access costs rather than continuing to
remove small amounts of surrounding overhead.
