import unittest
from datetime import datetime, timezone

from rampflow.models import Decision
from rampflow.event_stream import CursorExpired, EventStream
from rampflow.ops_store import VersionConflict, VersionedStore
from rampflow.workflows.control import RampWorkflowControl, WorkflowRejected


AT = datetime(2026, 1, 1, 10, tzinfo=timezone.utc)


class StreamProjectionTests(unittest.TestCase):
    def test_expired_cursor_is_rejected_and_carries_full_reset_cursor(self):
        stream = EventStream(retention=3)
        for sequence in range(1, 5):
            stream.publish(f"e{sequence}", AT, "ops", "F1", {"n": sequence})

        self.assertEqual(stream.floor, 2)
        boundary = stream.page(1)
        self.assertFalse(boundary.gap)
        self.assertEqual([event.sequence for event in boundary.events], [2, 3, 4])

        with self.assertRaises(CursorExpired) as captured:
            stream.page(0)

        error = captured.exception
        self.assertEqual(error.cursor, 0)
        self.assertEqual(error.floor, 2)
        self.assertEqual(error.reset_cursor, 1)

        # The compatibility path must still make the gap explicit; it must not
        # present retained events as a normal continuation.
        page = stream.page(0, strict=False)
        self.assertTrue(page.gap)
        self.assertEqual(page.reset_cursor, 1)
        self.assertEqual([event.sequence for event in page.events], [2, 3, 4])

    def test_wait_page_rejects_expired_cursor(self):
        stream = EventStream(retention=2)
        stream.publish("e1", AT, "ops", "F1", {})
        stream.publish("e2", AT, "ops", "F1", {})
        stream.publish("e3", AT, "ops", "F1", {})

        with self.assertRaises(CursorExpired):
            stream.wait_page(0, timeout_seconds=0)

    def test_batch_commit_is_all_or_nothing(self):
        store = VersionedStore()
        store.create("a", 0, "test", "seed", AT)
        store.create("b", 0, "test", "seed", AT)

        with self.assertRaises(VersionConflict):
            store.batch(
                (
                    ("put", "a", 1, 1),
                    ("put", "b", 2, 99),
                ),
                "test",
                "mixed batch",
                AT,
            )

        self.assertEqual(store.get("a").value, 0)
        self.assertEqual(store.get("b").value, 0)
        self.assertEqual(store.generation, 2)
        self.assertEqual(len(store.changes()), 2)

        committed = store.batch(
            (
                ("put", "a", 1, 1),
                ("put", "b", 2, 1),
            ),
            "test",
            "valid batch",
            AT,
        )
        self.assertEqual([record.value for record in committed], [1, 2])

    def test_projection_acknowledgement_commits_before_external_event(self):
        control = RampWorkflowControl()
        calls = []

        def fail():
            calls.append("failed")
            raise RuntimeError("projection failed")

        with self.assertRaises(RuntimeError):
            control.acknowledge_stream_projection(
                "ack-1", "projection-1", AT, fail, cursor=10
            )
        self.assertEqual(calls, ["failed"])
        self.assertEqual(control.stream.head, 0)
        self.assertEqual(control.receipts, {})

        receipt = control.acknowledge_stream_projection(
            "ack-2", "projection-1", AT, lambda: calls.append("committed") or "ok", cursor=11
        )
        self.assertEqual(calls, ["failed", "committed"])
        self.assertEqual(control.stream.head, 1)
        self.assertEqual(receipt.event.payload["cursor"], 11)
        self.assertEqual(receipt.result, "ok")

        control.acknowledge_stream_projection(
            "ack-2", "projection-1", AT, lambda: calls.append("replayed"), cursor=11
        )
        self.assertEqual(calls, ["failed", "committed"])

    def test_projection_acknowledgement_respects_requirements(self):
        control = RampWorkflowControl()

        with self.assertRaises(WorkflowRejected):
            control.acknowledge_stream_projection(
                "ack-3",
                "projection-1",
                AT,
                lambda: self.fail("action must not run"),
                requirements=(Decision(False, "stale", "reset required"),),
            )
        self.assertEqual(control.stream.head, 0)


if __name__ == "__main__":
    unittest.main()
