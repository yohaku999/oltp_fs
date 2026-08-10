from pathlib import Path
import tempfile
import unittest

import pandas as pd

try:
    from .buffer_pool_event_analysis import load_buffer_pool_event_aggregates
except ImportError:
    from buffer_pool_event_analysis import load_buffer_pool_event_aggregates


class BufferPoolEventAnalysisTest(unittest.TestCase):
    def test_streams_execute_interval_and_reuses_cache(self):
        with tempfile.TemporaryDirectory() as temp_dir:
            result_dir = Path(temp_dir)
            (result_dir / "benchbase.log").write_text(
                "\n".join(
                    [
                        "[INFO ] 2026-07-29 00:00:01,000 MEASURE",
                        "[INFO ] 2026-07-29 00:00:03,000 TERMINATE",
                    ]
                ),
                encoding="utf-8",
            )
            start_ms = int(
                pd.Timestamp("2026-07-29T00:00:00Z").timestamp() * 1000
            )
            event_lines = [
                self.event_line(start_ms, 1, "pin", 0, 3, "heap", 0, 0),
                self.event_line(start_ms + 1100, 2, "pin", 1, 3, "heap", 0, 1),
                self.event_line(start_ms + 1200, 3, "pin", 2, 4, "heap", 0, 0),
                self.event_line(start_ms + 1300, 4, "read", 2, 4, "heap", 0),
                self.event_line(start_ms + 2200, 5, "evict", 3, 5, "index", 1),
                self.event_line(start_ms + 3100, 6, "pin", 4, 6, "heap", 0, 1),
            ]
            (result_dir / "dbfs.buffer_pool_events.log").write_text(
                "\n".join(event_lines), encoding="utf-8"
            )

            first = load_buffer_pool_event_aggregates(
                result_dir, progress_every_lines=0
            )

            self.assertFalse(first["metadata"]["cache_hit"])
            self.assertEqual(first["metadata"]["matched_events"], 6)
            self.assertEqual(first["metadata"]["selected_events"], 4)
            self.assertEqual(first["summary"]["events"].sum(), 4)
            self.assertEqual(first["per_second"]["events"].sum(), 4)
            self.assertEqual(first["per_page"]["events"].sum(), 4)
            self.assertEqual(first["per_frame"]["events"].sum(), 2)
            self.assertEqual(len(first["last_events"]), 4)
            self.assertEqual(
                sorted(first["per_second"]["elapsed_second"].unique().tolist()),
                [0, 1],
            )

            second = load_buffer_pool_event_aggregates(
                result_dir, progress_every_lines=0
            )

            self.assertTrue(second["metadata"]["cache_hit"])
            self.assertEqual(second["metadata"]["selected_events"], 4)
            self.assertEqual(second["per_page"]["events"].sum(), 4)

    @staticmethod
    def event_line(
        unix_ms,
        event_id,
        event_type,
        page_id,
        frame_id,
        page_kind,
        dirty,
        hit=None,
    ):
        line = (
            f"logged_at_unix_ms={unix_ms} buffer_pool_event "
            f"type={event_type} event_id={event_id} file=data/test.db "
            f"page_id={page_id} frame_id={frame_id} "
            f"page_kind={page_kind} dirty={dirty}"
        )
        if hit is not None:
            line += f" hit={hit}"
        return line


if __name__ == "__main__":
    unittest.main()
