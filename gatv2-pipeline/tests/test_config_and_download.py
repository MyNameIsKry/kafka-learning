import io
import tempfile
import unittest
import zipfile
from pathlib import Path
from unittest.mock import patch

from paysim_pipeline.config import load_config
import download_dataset


def dataset_zip() -> bytes:
    stream = io.BytesIO()
    with zipfile.ZipFile(stream, "w", zipfile.ZIP_DEFLATED) as archive:
        archive.writestr(download_dataset.EXPECTED_CSV, ",".join(sorted(download_dataset.EXPECTED_COLUMNS)) + "\n")
    return stream.getvalue()


class ConfigAndDownloadTests(unittest.TestCase):
    def test_config_resolves_paths_and_validates_split_fractions(self):
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            config_path = root / "configs" / "pipeline.toml"
            config_path.parent.mkdir()
            config_text = (
                '[dataset]\ninput_csv = "data/input.csv"\noutput_dir = "artifacts"\ndataset_version = "v1"\n'
                '[split]\ntrain_fraction = 0.7\nvalidation_fraction = 0.15\ntest_fraction = 0.15\n'
                '[pipeline]\nstages = ["load", "eda", "validate_clean", "chronological_split", "export"]\n'
            )
            config_path.write_text(config_text, encoding="utf-8")

            config = load_config(config_path)

            self.assertEqual(config.input_csv, root / "data" / "input.csv")
            self.assertEqual(config.output_dir, root / "artifacts")
            self.assertAlmostEqual(config.train_fraction + config.validation_fraction + config.test_fraction, 1.0)

            config_path.write_text(config_text.replace("test_fraction = 0.15", "test_fraction = 0.2"), encoding="utf-8")
            with self.assertRaisesRegex(ValueError, "must be positive and sum to 1"):
                load_config(config_path)

    def test_download_creates_directory_extracts_and_validates_csv_without_network(self):
        with tempfile.TemporaryDirectory() as directory:
            output = Path(directory) / "new-data-dir"
            response = io.BytesIO(dataset_zip())
            with patch.object(download_dataset.urllib.request, "urlopen", return_value=response) as urlopen:
                csv_path = download_dataset.download(output, force=False)
                self.assertEqual(csv_path, output / download_dataset.EXPECTED_CSV)
                self.assertTrue(download_dataset.valid_csv(csv_path))

        urlopen.assert_called_once()
        self.assertEqual(urlopen.call_args.kwargs["timeout"], 60)

    def test_download_skips_existing_valid_file(self):
        with tempfile.TemporaryDirectory() as directory:
            output = Path(directory) / "data"
            output.mkdir()
            csv_path = output / download_dataset.EXPECTED_CSV
            csv_path.write_text(",".join(download_dataset.EXPECTED_COLUMNS) + "\n", encoding="utf-8")
            with patch.object(download_dataset.urllib.request, "urlopen") as urlopen:
                self.assertEqual(download_dataset.download(output, force=False), csv_path)
        urlopen.assert_not_called()

    def test_safe_extract_rejects_path_traversal(self):
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            archive_path = root / "unsafe.zip"
            with zipfile.ZipFile(archive_path, "w") as archive:
                archive.writestr("../escape.csv", "bad")

            with zipfile.ZipFile(archive_path) as archive:
                with self.assertRaisesRegex(ValueError, "Unsafe path"):
                    download_dataset.safe_extract(archive, root / "extracted")


if __name__ == "__main__":
    unittest.main()
