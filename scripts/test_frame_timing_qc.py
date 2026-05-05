#!/usr/bin/env python3
import sys
import unittest
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent))

from frame_timing_qc import (
    FILTER_OUT,
    OK,
    PERFECT,
    analyze_timestamps,
    is_firmware_eligible,
)


def timestamps_from_deltas_ms(deltas_ms):
    timestamps = [0.0]
    for delta in deltas_ms:
        timestamps.append(timestamps[-1] + delta / 1000)
    return timestamps


class FirmwareVersionTests(unittest.TestCase):
    def test_firmware_eligibility_boundaries(self):
        for version in ("7.4.3", "7.4.4", "7.5.0", "v7.4.3-beta"):
            self.assertTrue(is_firmware_eligible(version), version)
        for version in ("7.4.2", "7.0.12", None, "", "bad.version"):
            self.assertFalse(is_firmware_eligible(version), version)


class FrameTimingClassificationTests(unittest.TestCase):
    def test_perfect_30fps(self):
        result = analyze_timestamps([i / 30 for i in range(300)])
        self.assertEqual(result["bucket"], PERFECT)
        self.assertEqual(result["gap_pct"], 0)
        self.assertLess(result["max_delta_ms"], 50)

    def test_ok_with_sparse_single_gap(self):
        deltas = [1000 / 30] * 299
        deltas[10] = 66.667
        result = analyze_timestamps(timestamps_from_deltas_ms(deltas))
        self.assertEqual(result["bucket"], OK)
        self.assertEqual(result["single_gaps"], 1)
        self.assertLess(result["gap_pct"], 2.0)

    def test_filter_out_stable_lower_effective_fps(self):
        result = analyze_timestamps(timestamps_from_deltas_ms([40] * 99))
        self.assertEqual(result["bucket"], FILTER_OUT)
        self.assertEqual(result["gap_pct"], 0)
        self.assertIn("effective_fps_lt_29", result["failed_rules"])

    def test_ok_at_gap_pct_boundary(self):
        deltas = [1000 / 30] * 50
        deltas[10] = 66.667
        result = analyze_timestamps(timestamps_from_deltas_ms(deltas))
        self.assertEqual(result["bucket"], OK)
        self.assertEqual(result["gap_pct"], 2.0)

    def test_ok_ignores_scattered_gap_pct_when_cluster_rule_passes(self):
        deltas = [1000 / 30] * 299
        for idx in (10, 50, 90, 130, 170, 210, 250):
            deltas[idx] = 66.667
        result = analyze_timestamps(timestamps_from_deltas_ms(deltas))
        self.assertEqual(result["bucket"], OK)
        self.assertGreater(result["gap_pct"], 2.0)
        self.assertLessEqual(result["max_late_frames_per_2s"], 4)
        self.assertEqual(result["failed_rules"], [])

    def test_filter_out_clustered_single_frame_gaps(self):
        deltas = [1000 / 30] * 899
        for idx in (600, 605, 610, 615, 620):
            deltas[idx] = 66.667
        result = analyze_timestamps(timestamps_from_deltas_ms(deltas))
        self.assertEqual(result["bucket"], FILTER_OUT)
        self.assertEqual(result["single_gaps"], 5)
        self.assertEqual(result["double_gaps"], 0)
        self.assertEqual(result["max_delta_ms"], 66.667)
        self.assertEqual(result["max_late_frames_per_2s"], 5)
        self.assertEqual(result["late_frame_clusters"], 1)
        self.assertIn("late_frame_cluster_gte_5_in_2s", result["failed_rules"])

    def test_ok_with_four_single_frame_gaps_in_cluster_window(self):
        deltas = [1000 / 30] * 899
        for idx in (600, 605, 610, 615):
            deltas[idx] = 66.667
        result = analyze_timestamps(timestamps_from_deltas_ms(deltas))
        self.assertEqual(result["bucket"], OK)
        self.assertEqual(result["max_late_frames_per_2s"], 4)
        self.assertEqual(result["late_frame_clusters"], 0)

    def test_filter_out_one_double_gap_over_100ms(self):
        deltas = [1000 / 30] * 99
        deltas[10] = 101
        result = analyze_timestamps(timestamps_from_deltas_ms(deltas))
        self.assertEqual(result["bucket"], FILTER_OUT)
        self.assertEqual(result["double_gaps"], 1)
        self.assertEqual(result["triple_plus_gaps"], 0)
        self.assertIn("max_delta_ms_gt_100", result["failed_rules"])

    def test_filter_out_double_gaps(self):
        deltas = [1000 / 30] * 499
        for idx in (10, 20):
            deltas[idx] = 91
        result = analyze_timestamps(timestamps_from_deltas_ms(deltas))
        self.assertEqual(result["bucket"], FILTER_OUT)
        self.assertEqual(result["double_gaps"], 2)
        self.assertIn("double_gaps_gte_2", result["failed_rules"])

    def test_filter_out_triple_plus_gap(self):
        deltas = [1000 / 30] * 499
        deltas[10] = 131
        result = analyze_timestamps(timestamps_from_deltas_ms(deltas))
        self.assertEqual(result["bucket"], FILTER_OUT)
        self.assertEqual(result["double_gaps"], 0)
        self.assertEqual(result["triple_plus_gaps"], 1)
        self.assertIn("triple_plus_gaps_gt_0", result["failed_rules"])

    def test_ok_with_one_sub_100ms_double_gap(self):
        deltas = [1000 / 30] * 899
        for idx in (10, 110, 210, 310, 410, 510, 610):
            deltas[idx] = 66.667
        deltas[80] = 99.9
        result = analyze_timestamps(timestamps_from_deltas_ms(deltas))
        self.assertEqual(result["bucket"], OK)
        self.assertEqual(result["double_gaps"], 1)
        self.assertLess(result["gap_pct"], 2.0)

    def test_ok_at_100ms_boundary(self):
        deltas = [1000 / 30] * 899
        deltas[80] = 100
        result = analyze_timestamps(timestamps_from_deltas_ms(deltas))
        self.assertEqual(result["bucket"], OK)
        self.assertEqual(result["double_gaps"], 1)
        self.assertEqual(result["failed_rules"], [])

    def test_delta_boundaries(self):
        result_50 = analyze_timestamps(timestamps_from_deltas_ms([1000 / 30] * 20 + [50] + [1000 / 30] * 279))
        self.assertEqual(result_50["bucket"], OK)
        self.assertEqual(result_50["gap_pct"], 0)
        self.assertEqual(result_50["single_gaps"], 0)

        result_55 = analyze_timestamps(timestamps_from_deltas_ms([1000 / 30] * 20 + [55] + [1000 / 30] * 279))
        self.assertEqual(result_55["bucket"], OK)
        self.assertEqual(result_55["single_gaps"], 0)

        result_90 = analyze_timestamps(timestamps_from_deltas_ms([1000 / 30] * 20 + [90] + [1000 / 30] * 279))
        self.assertEqual(result_90["bucket"], OK)
        self.assertEqual(result_90["single_gaps"], 1)

        result_100 = analyze_timestamps(timestamps_from_deltas_ms([1000 / 30] * 20 + [100] + [1000 / 30] * 279))
        self.assertEqual(result_100["bucket"], OK)
        self.assertEqual(result_100["double_gaps"], 1)

        result_100_9 = analyze_timestamps(timestamps_from_deltas_ms([1000 / 30] * 20 + [100.9] + [1000 / 30] * 279))
        self.assertEqual(result_100_9["bucket"], OK)
        self.assertEqual(result_100_9["double_gaps"], 1)
        self.assertEqual(result_100_9["failed_rules"], [])

        result_101 = analyze_timestamps(timestamps_from_deltas_ms([1000 / 30] * 20 + [101] + [1000 / 30] * 279))
        self.assertEqual(result_101["bucket"], FILTER_OUT)
        self.assertEqual(result_101["double_gaps"], 1)
        self.assertIn("max_delta_ms_gt_100", result_101["failed_rules"])

        result_130_9 = analyze_timestamps(timestamps_from_deltas_ms([1000 / 30] * 20 + [130.9] + [1000 / 30] * 279))
        self.assertEqual(result_130_9["bucket"], FILTER_OUT)
        self.assertEqual(result_130_9["double_gaps"], 1)
        self.assertEqual(result_130_9["triple_plus_gaps"], 0)
        self.assertIn("max_delta_ms_gt_100", result_130_9["failed_rules"])

        result_131 = analyze_timestamps(timestamps_from_deltas_ms([1000 / 30] * 20 + [131] + [1000 / 30] * 279))
        self.assertEqual(result_131["bucket"], FILTER_OUT)
        self.assertEqual(result_131["double_gaps"], 0)
        self.assertEqual(result_131["triple_plus_gaps"], 1)
        self.assertIn("triple_plus_gaps_gt_0", result_131["failed_rules"])


if __name__ == "__main__":
    unittest.main()
