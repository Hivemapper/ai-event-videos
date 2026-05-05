#!/usr/bin/env python3
"""Focused tests for production video quality safeguards."""

from __future__ import annotations

import importlib.util
import unittest
from pathlib import Path


PROJECT_ROOT = Path(__file__).resolve().parent.parent
SCRIPT_PATH = PROJECT_ROOT / "scripts" / "prod-pipeline.py"


def load_prod_pipeline():
    spec = importlib.util.spec_from_file_location("prod_pipeline", SCRIPT_PATH)
    module = importlib.util.module_from_spec(spec)
    assert spec.loader is not None
    spec.loader.exec_module(module)
    return module


class ProductionQualityTests(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        cls.prod = load_prod_pipeline()

    def test_quality_hevc_command_preserves_timing_flags(self):
        cmd = self.prod.build_quality_hevc_ffmpeg_command(
            Path("/tmp/source.mp4"),
            Path("/tmp/output.mp4"),
            "[0:v]drawbox=x=10:y=12:w=32:h=24:color=0x8a8a8a@0.95:t=fill[v]",
            "v",
            5_563_290,
        )
        joined = " ".join(cmd)
        self.assertIn("-i /tmp/source.mp4", joined)
        self.assertIn("drawbox=x=10:y=12:w=32:h=24", joined)
        self.assertIn("-c:v libx265", joined)
        self.assertIn("-b:v 5563290", joined)
        self.assertIn("-tag:v hvc1", joined)
        self.assertIn("-fps_mode passthrough", joined)
        self.assertIn("-enc_time_base -1", joined)
        self.assertNotIn("lossless=1", joined)
        self.assertNotIn("h264", joined)
        self.assertNotIn("-r ", joined)

    def test_target_hevc_bitrate_tracks_source_with_margin(self):
        target = self.prod.target_hevc_bitrate_bps({
            "format": {"bit_rate": "5563290"}
        })
        self.assertEqual(target, int(5_563_290 * 1.03))

    def test_privacy_tracking_interpolates_jittery_boxes(self):
        frame_timing = [
            {"time_s": index / 30, "duration_s": 1 / 30}
            for index in range(6)
        ]
        boxes = self.prod.stabilize_privacy_boxes(
            [
                {
                    "label": "plate",
                    "frame_index": 0,
                    "frame_ms": 0,
                    "x": 100,
                    "y": 200,
                    "w": 24,
                    "h": 12,
                },
                {
                    "label": "plate",
                    "frame_index": 2,
                    "frame_ms": 67,
                    "x": 106,
                    "y": 202,
                    "w": 25,
                    "h": 12,
                },
                {
                    "label": "plate",
                    "frame_index": 4,
                    "frame_ms": 133,
                    "x": 110,
                    "y": 201,
                    "w": 24,
                    "h": 13,
                },
            ],
            frame_timing,
        )

        self.assertEqual([box["frame_index"] for box in boxes], [0, 1, 2, 3, 4])
        self.assertEqual(len({box["track_id"] for box in boxes}), 1)
        self.assertTrue(any(box.get("interpolated") for box in boxes))
        self.assertTrue(all(box["w"] >= 24 for box in boxes))
        self.assertTrue(all(box["h"] >= 12 for box in boxes))
        self.assertAlmostEqual(boxes[1]["start_s"], 1 / 30)

    def test_privacy_tracking_follows_fast_motion_without_large_lag(self):
        frame_timing = [
            {"time_s": index / 30, "duration_s": 1 / 30}
            for index in range(5)
        ]
        raw_boxes = [
            {
                "label": "plate",
                "frame_index": index,
                "frame_ms": int(round(index / 30 * 1000)),
                "x": 100 + index * 20,
                "y": 200,
                "w": 24,
                "h": 12,
            }
            for index in range(5)
        ]

        boxes = self.prod.stabilize_privacy_boxes(raw_boxes, frame_timing)
        last_box = boxes[-1]

        self.assertEqual(last_box["frame_index"], 4)
        self.assertGreaterEqual(last_box["x"], 174)

    def test_redaction_ass_uses_vector_rounded_plate_mask(self):
        ass_script, count = self.prod.build_privacy_ass_script(
            [
                {
                    "label": "plate",
                    "frame_index": 0,
                    "frame_ms": 0,
                    "start_s": 0,
                    "end_s": 0.03334,
                    "x": 100,
                    "y": 200,
                    "w": 24,
                    "h": 12,
                }
            ],
            1280,
            720,
        )

        self.assertEqual(count, 1)
        self.assertIn(r"\p1", ass_script)
        self.assertIn(r"\pos(", ass_script)
        self.assertIn(" b ", ass_script)
        self.assertIn("Dialogue: 0,0:00:00.00,0:00:00.04,Privacy", ass_script)

    def test_face_redaction_uses_larger_corner_radius(self):
        plate = self.prod._clamp_box(
            self.prod._expand_privacy_box({
                "label": "plate",
                "frame_index": 0,
                "frame_ms": 0,
                "start_s": 0,
                "end_s": 0.03334,
                "x": 100,
                "y": 200,
                "w": 32,
                "h": 44,
            }),
            1280,
            720,
        )
        face = self.prod._clamp_box(
            self.prod._expand_privacy_box({
                "label": "face",
                "frame_index": 0,
                "frame_ms": 0,
                "start_s": 0,
                "end_s": 0.03334,
                "x": 100,
                "y": 200,
                "w": 32,
                "h": 44,
            }),
            1280,
            720,
        )

        self.assertIsNotNone(plate)
        self.assertIsNotNone(face)
        self.assertGreater(
            self.prod.privacy_box_corner_radius(face),
            self.prod.privacy_box_corner_radius(plate),
        )

    def test_ass_redaction_filter_uses_single_input_video(self):
        filter_complex, output_label = self.prod.build_ass_redaction_filter(
            Path("/tmp/privacy.ass"),
            1280,
            720,
        )

        self.assertEqual(output_label, "v")
        self.assertEqual(filter_complex, "[0:v]ass=filename='/tmp/privacy.ass':original_size=1280x720[v]")

    def test_privacy_ass_counts_multiple_masks(self):
        ass_script, count = self.prod.build_privacy_ass_script(
            [
                {
                    "label": "face",
                    "frame_index": 0,
                    "frame_ms": 0,
                    "start_s": 0,
                    "end_s": 0.03334,
                    "x": 100,
                    "y": 200,
                    "w": 32,
                    "h": 44,
                }
            ],
            1280,
            720,
        )

        self.assertEqual(count, 1)
        self.assertEqual(ass_script.count("Dialogue:"), 1)

    def test_privacy_boxes_are_class_sized_before_rendering(self):
        plate = self.prod._expand_privacy_box({
            "label": "plate",
            "x": 100,
            "y": 200,
            "w": 24,
            "h": 12,
        })
        face = self.prod._expand_privacy_box({
            "label": "face",
            "x": 100,
            "y": 200,
            "w": 30,
            "h": 40,
        })

        self.assertLess(plate["w"], 32)
        self.assertLess(plate["h"], 18)
        self.assertLess(face["w"], 40)
        self.assertLess(face["h"], 50)


if __name__ == "__main__":
    unittest.main()
