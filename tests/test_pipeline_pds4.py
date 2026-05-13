import copy
import shutil

import pytest
import yaml

from pathlib import Path

from nirmir_pipeline.pipeline.run import run_generate_pds4


# ─── shared helpers ───────────────────────────────────────────────────────────

@pytest.fixture
def repo_root(pytestconfig) -> Path:
    return pytestconfig.rootpath


def read_yaml(path: Path) -> dict:
    return yaml.safe_load(path.read_text(encoding="utf-8"))


def write_yaml(path: Path, data: dict) -> None:
    path.write_text(yaml.safe_dump(data, sort_keys=False), encoding="utf-8")


@pytest.fixture
def flat_fits_dir(tmp_path: Path, repo_root: Path) -> Path:
    """Copy all test FITS fixtures into a single flat directory, mirroring real pipeline output."""
    dest = tmp_path / "fits_input"
    dest.mkdir()
    for f in (repo_root / "tests" / "data" / "fits").rglob("*.fits"):
        shutil.copy2(f, dest / f.name)
    return dest


def _write_pds4_config(
    tmp_path: Path,
    repo_root: Path,
    input_dir: Path,
    products: list[str],
    channels: list[str],
) -> Path:
    """
    Read tests/configs/test.yaml, override the pds4 section with absolute paths
    and caller-supplied products/channels, and write the result to tmp_path.
    """
    base = read_yaml(repo_root / "tests" / "configs" / "test.yaml")
    cfg = copy.deepcopy(base)
    cfg["pds4"]["input"] = str(input_dir)
    cfg["pds4"]["templates_dir"] = str(repo_root / "pds4_templates")
    cfg["pds4"]["output"] = str(tmp_path / "pds4_out")
    cfg["pds4"]["products"] = products
    cfg["pds4"]["channels"] = channels
    config_path = tmp_path / "test_pds4.yaml"
    write_yaml(config_path, cfg)
    return config_path


# ─── product structure ────────────────────────────────────────────────────────

class TestPds4ProductStructure:
    """Each generated product bundle must contain exactly one XML label and one FITS file."""

    def test_each_product_has_xml_label_and_fits(self, tmp_path, repo_root, flat_fits_dir):
        config = _write_pds4_config(tmp_path, repo_root, flat_fits_dir, ["0A"], ["NIR", "MIR"])
        run_generate_pds4(config)

        output_dir = tmp_path / "pds4_out"
        assert output_dir.exists(), "PDS4 output directory was not created"

        product_dirs = [d for d in output_dir.iterdir() if d.is_dir()]
        assert product_dirs, "No PDS4 product subdirectories were created"

        for product_dir in product_dirs:
            stem = product_dir.name
            xml_files = list(product_dir.glob("*.xml"))
            fits_files = list(product_dir.glob("*.fits"))

            assert len(xml_files) == 1, \
                f"{stem}: expected 1 XML label, got {len(xml_files)}"
            assert len(fits_files) == 1, \
                f"{stem}: expected 1 FITS file, got {len(fits_files)}"
            # Label filename is always CI_MIRMIS_<stem>.xml
            assert xml_files[0].name == f"CI_MIRMIS_{stem}.xml", \
                f"{stem}: unexpected label filename '{xml_files[0].name}'"

    def test_label_contains_pds4_structure(self, tmp_path, repo_root, flat_fits_dir):
        """Generated XML labels include mandatory PDS4 root and file-area elements."""
        config = _write_pds4_config(tmp_path, repo_root, flat_fits_dir, ["0A"], ["NIR"])
        run_generate_pds4(config)

        for xml_file in (tmp_path / "pds4_out").rglob("*.xml"):
            content = xml_file.read_text(encoding="utf-8")
            assert "Product_Observational" in content, \
                f"{xml_file.name}: missing PDS4 root element 'Product_Observational'"
            assert "File_Area_Observational" in content, \
                f"{xml_file.name}: missing 'File_Area_Observational'"


# ─── level coverage ───────────────────────────────────────────────────────────

class TestPds4LevelCoverage:
    """Products must be generated for every present level and silently skipped for absent ones."""

    def test_all_present_levels_produce_products(self, tmp_path, repo_root, flat_fits_dir):
        """Every level that has a FITS file in the input directory generates a product."""
        # Test data covers: 0A (NIR+MIR), 1A (NIR+MIR).
        # MIR 1B has a stale FILENAME header so is excluded from this check.
        config = _write_pds4_config(tmp_path, repo_root, flat_fits_dir, ["0A", "1A"], ["NIR", "MIR"])
        run_generate_pds4(config)

        stems = {d.name for d in (tmp_path / "pds4_out").iterdir() if d.is_dir()}

        for level in ["0A", "1A"]:
            assert any(s.endswith(level) for s in stems), \
                f"No product generated for level {level}; found dirs: {stems}"

    def test_missing_levels_are_silently_skipped(self, tmp_path, repo_root, flat_fits_dir):
        """Levels with no matching FITS file complete without raising and produce no output."""
        # 1A-extra and 1C have no test fixtures
        config = _write_pds4_config(
            tmp_path, repo_root, flat_fits_dir,
            ["0A", "1A", "1A-extra", "1B", "1C"],
            ["NIR", "MIR"],
        )

        run_generate_pds4(config)  # must not raise

        stems = {d.name for d in (tmp_path / "pds4_out").iterdir() if d.is_dir()}

        for present in ["0A", "1A"]:
            assert any(s.endswith(present) for s in stems), \
                f"Expected a product for present level {present}; found: {stems}"

        for absent in ["1A-extra", "1C"]:
            assert not any(s.endswith(absent) for s in stems), \
                f"Unexpected product for absent level {absent}"

    def test_empty_input_directory_does_not_raise(self, tmp_path, repo_root):
        """An input directory with no FITS files produces no output and does not raise."""
        empty_dir = tmp_path / "empty"
        empty_dir.mkdir()
        config = _write_pds4_config(
            tmp_path, repo_root, empty_dir,
            ["0A", "1A", "1B"],
            ["NIR", "MIR"],
        )

        run_generate_pds4(config)  # must not raise

        output_dir = tmp_path / "pds4_out"
        if output_dir.exists():
            assert not list(output_dir.iterdir()), \
                "Expected empty output directory when input has no FITS files"


# ─── channel handling ─────────────────────────────────────────────────────────

class TestPds4ChannelHandling:
    """Products for each channel are generated and filtered independently."""

    def test_both_channels_produce_separate_products(self, tmp_path, repo_root, flat_fits_dir):
        """When both channels are requested, each channel produces at least one product."""
        config = _write_pds4_config(tmp_path, repo_root, flat_fits_dir, ["0A"], ["NIR", "MIR"])
        run_generate_pds4(config)

        stems = {d.name for d in (tmp_path / "pds4_out").iterdir() if d.is_dir()}
        nir_products = {s for s in stems if s.startswith("NIR_")}
        mir_products = {s for s in stems if s.startswith("MIR_")}

        assert nir_products, "No NIR products generated"
        assert mir_products, "No MIR products generated"

    def test_single_channel_request_excludes_other_channel(self, tmp_path, repo_root, flat_fits_dir):
        """When only NIR is requested from a mixed input, no MIR products are created."""
        config = _write_pds4_config(tmp_path, repo_root, flat_fits_dir, ["0A", "1A"], ["NIR"])
        run_generate_pds4(config)

        for product_dir in (tmp_path / "pds4_out").iterdir():
            assert product_dir.name.startswith("NIR_"), \
                f"Expected only NIR products; found '{product_dir.name}'"

    def test_absent_channel_in_input_is_skipped_without_error(self, tmp_path, repo_root, flat_fits_dir):
        """Requesting a channel that has no FITS files in the input completes without error."""
        nir_only = tmp_path / "nir_only"
        nir_only.mkdir()
        for f in flat_fits_dir.glob("NIR_*.fits"):
            shutil.copy2(f, nir_only / f.name)

        # Only MIR requested, but the input directory has no MIR files
        config = _write_pds4_config(
            tmp_path, repo_root, nir_only,
            ["0A", "1A", "1B"],
            ["MIR"],
        )

        run_generate_pds4(config)  # must not raise

        output_dir = tmp_path / "pds4_out"
        if output_dir.exists():
            assert not list(output_dir.iterdir()), \
                "Expected no output when only absent-channel FITS files are requested"
