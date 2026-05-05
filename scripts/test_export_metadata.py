#!/usr/bin/env python3
"""Focused tests for production metadata export helpers."""

from __future__ import annotations

import importlib.util
import unittest
from pathlib import Path


PROJECT_ROOT = Path(__file__).resolve().parent.parent
SCRIPT_PATH = PROJECT_ROOT / "scripts" / "export-metadata.py"


def load_export_metadata():
    spec = importlib.util.spec_from_file_location("export_metadata", SCRIPT_PATH)
    module = importlib.util.module_from_spec(spec)
    assert spec.loader is not None
    spec.loader.exec_module(module)
    return module


class ExportMetadataTests(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        cls.export_metadata = load_export_metadata()

    def test_video_metadata_from_probe_uses_snake_case_contract(self):
        probe = {
            "format": {
                "format_name": "mov,mp4,m4a,3gp,3g2,mj2",
                "duration": "31.050000",
                "size": "21779282",
                "bit_rate": "5611663",
            },
            "streams": [
                {
                    "codec_type": "video",
                    "codec_name": "hevc",
                    "width": 1280,
                    "height": 720,
                    "nb_read_frames": "930",
                    "bit_rate": "5580000",
                }
            ],
        }

        metadata = self.export_metadata.video_metadata_from_probe(probe, "/tmp/example.mp4")

        self.assertEqual(metadata, {
            "codec": "hevc",
            "container": "mp4",
            "width": 1280,
            "height": 720,
            "frame_count": 930,
            "bitrate_bps": 5611663,
            "size_bytes": 21779282,
        })

    def test_pts_us_from_frame_times_normalizes_to_zero(self):
        pts_us = self.export_metadata.pts_us_from_frame_times([
            12.5,
            12.533333,
            12.566667,
        ])

        self.assertEqual(pts_us, [0, 33333, 66667])

    def test_validate_metadata_accepts_real_vfr_spread(self):
        self.export_metadata.validate_metadata({
            "video": {"frame_count": 4},
            "pts_us": [0, 33333, 66666, 133333],
        })

    def test_validate_metadata_rejects_frame_count_mismatch(self):
        with self.assertLogs("export_metadata", level="ERROR"):
            with self.assertRaisesRegex(ValueError, "frame_count"):
                self.export_metadata.validate_metadata({
                    "video": {"frame_count": 4},
                    "pts_us": [0, 33333, 66666],
                })

    def test_validate_metadata_rejects_cfr_timestamps(self):
        with self.assertRaisesRegex(ValueError, "CFR"):
            self.export_metadata.validate_metadata({
                "video": {"frame_count": 4},
                "pts_us": [0, 33333, 66666, 99999],
            })

    def test_validate_metadata_can_allow_low_pts_spread_for_known_good_export(self):
        self.export_metadata.validate_metadata(
            {
                "video": {"frame_count": 4},
                "pts_us": [0, 33333, 66666, 99999],
            },
            allow_low_pts_spread=True,
        )


if __name__ == "__main__":
    unittest.main()
