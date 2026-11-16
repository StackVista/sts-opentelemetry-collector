import unittest

from oracle import assert_objects


class ObjectOracleTest(unittest.TestCase):
    def test_complete_snapshot_rejects_duplicate_overlap(self):
        obj = {
            "apiVersion": "collection-test.example.com/v1",
            "kind": "Widget",
            "metadata": {"namespace": "rc-team-a", "name": "both", "uid": "one"},
        }
        record = {"body": {"object": obj, "type": "ADDED"}, "attributes": {}}
        with self.assertRaisesRegex(AssertionError, "duplicate"):
            assert_objects([record, record], ["widgets/rc-team-a/both"], unique=True)


if __name__ == "__main__":
    unittest.main()
