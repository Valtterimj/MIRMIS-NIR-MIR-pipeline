# Comet Interceptor MIRMIS NIR-MIR Pipeline

## Overview

This repository contains the ground data processing pipeline for the MIRMIS instrument's NIR (near-infrared hyperspectral imager) and MIR (mid-infrared spectrometer) channels aboard ESA's [Comet Interceptor](https://www.cosmos.esa.int/web/comet-interceptor) mission. The pipeline processes raw binary instrument data from the NIR hyperspectral camera and the MIR spectrometer through a sequence of calibration levels (0A → 1A → 1B → 1C), producing structured FITS data products and PDS4-compliant labels according to ESA standards.

The pipeline is operated from the command line via the `mirmis` CLI.


---

## Table of Contents

1. [Requirements](#requirements)
2. [Installation](#installation)
   - [Using uv (recommended)](#using-uv-recommended)
   - [Using pip](#using-pip)
   - [Compile the JPEG 2000 decompressor](#compile-the-jpeg-2000-decompressor)
3. [Repository Structure](#repository-structure)
4. [Input Data Format](#input-data-format)
5. [Calibration Files](#calibration-files)
6. [Configuration](#configuration)
7. [Running the Pipeline](#running-the-pipeline)
8. [Processing Levels](#processing-levels)
9. [Outputs](#outputs)
10. [Additional Resources](#additional-resources)
11. [Contact](#contact)

---

## Requirements

| Requirement | Notes |
|---|---|
| Python 3.12+ | |
| C compiler (`clang` or `gcc`) | Required only if raw files are JPEG 2000 compressed |
| [Jasper library](https://jasper-software.github.io/jasper-manual/latest/html/index.html) | Required only if raw files are JPEG 2000 compressed |

Python dependencies are managed via `pyproject.toml` and include: `astropy`, `numpy`, `scipy`, `xarray`, `matplotlib`, `pandas`, `spiceypy`, `pyyaml`, `netCDF4`, `h5netcdf`.

---

## Installation

### Using uv (recommended)

[uv](https://github.com/astral-sh/uv) is a fast Python package manager. It reads `uv.lock` to reproduce the exact dependency environment.

```bash
# 1. Clone the repository
git clone https://github.com/Valtterimj/MIRMIS-NIR-MIR-pipeline.git
cd MIRMIS-NIR-MIR-pipeline

# 2. Create the virtual environment and install all dependencies
uv venv
source .venv/bin/activate   # On Windows: .venv\Scripts\activate
uv sync
```

`uv sync` installs the package itself (including the `mirmis` CLI entry point) and all locked dependencies in one step.

### Using pip

This project uses a **`src` layout** — the package lives under `src/nirmir_pipeline/` rather than the project root. The `pyproject.toml` tells the build backend (hatchling) where to find it:

```toml
[tool.hatch.build.targets.wheel]
packages = ["src/nirmir_pipeline"]
```

Without installing the package properly, Python will **not** be able to resolve the `nirmir_pipeline` imports and the pipeline will fail with `ModuleNotFoundError`.

The correct way to install with pip is using **editable mode** (`-e`), which registers the `src/` directory on the Python path:

```bash
# 1. Clone the repository
git clone https://github.com/Valtterimj/MIRMIS-NIR-MIR-pipeline.git
cd MIRMIS-NIR-MIR-pipeline

# 2. Create and activate a virtual environment
python -m venv .venv
source .venv/bin/activate   # On Windows: .venv\Scripts\activate

# 3. Install the package and all dependencies in editable mode
pip install -e .
```

The `-e .` flag instructs pip to use `pyproject.toml` in the current directory. Hatchling uses this to correctly resolve `src/nirmir_pipeline` as the package root, and the `mirmis` CLI entry point will be registered in your environment.

> **Do not** attempt `pip install nirmir_pipeline` or manually add `src/` to `PYTHONPATH` — use `pip install -e .` from the project root.

### Compile the JPEG 2000 decompressor

Raw acquisition files may be JPEG 2000 compressed. In that case the pipeline uses a compiled C subprocess to decompress them on the fly. If your input files are uncompressed `.bin` files this step is not needed.

First, install the Jasper library if not already present:

- macOS: `brew install jasper`
- Ubuntu/Debian: `sudo apt-get install libjasper-dev`
- See the [Jasper documentation](https://jasper-software.github.io/jasper-manual/latest/html/index.html) for other platforms.

Then compile from the project root:

```bash
cd src/nirmir_pipeline/pipeline/levels/level_0
cc decompress.c -o decompress $(pkg-config --cflags --libs jasper)
```

The resulting `decompress` binary must remain in that directory — the pipeline locates it there at runtime.

---

## Repository Structure

```text
MIRMIS-NIR-MIR-pipeline/
│
├── pyproject.toml              # Project metadata, dependencies, build config
├── uv.lock                     # Locked dependency versions (uv)
├── README.md
│
├── configs/
│   ├── pipeline.yaml           # Default config (searched first at runtime)
│   └── pipeline.example.yaml  # Annotated example — copy and edit this
│
├── calibration/                # Calibration reference files
│   ├── DARKS/                  # Dark background frames (.fits)
│   ├── FLATS/                  # Flat field frames (.fits)
│   ├── BADPIXELS/              # Bad pixel lists (.txt)
│   ├── RADIANCE/               # Radiance conversion coefficients (.txt)
│   └── SOLAR/                  # Solar spectral irradiance data (.csv)
│
├── src/
│   └── nirmir_pipeline/
│       ├── cli.py              # CLI entry point (`mirmis` command)
│       ├── utils/
│       │   └── logging_config.py
│       └── pipeline/
│           ├── run.py          # Top-level pipeline orchestrator
│           ├── config.py       # YAML config loading and validation
│           ├── visualise.py    # FITS visualisation
│           ├── levels/
│           │   ├── level_0/    # Level 0: raw binary → FITS
│           │   │   ├── run.py
│           │   │   ├── build_fits.py
│           │   │   ├── metadata.py
│           │   │   ├── spice.py
│           │   │   ├── decompress.c    # JP2 decompressor source
│           │   │   └── decompress      # Compiled binary (after build step)
│           │   └── level_1/    # Level 1: calibration
│           │       ├── run.py
│           │       ├── calibrate_header.py
│           │       ├── extract_cds.py
│           │       ├── dark_background.py
│           │       ├── flat_field.py
│           │       ├── bad_pixels.py
│           │       ├── radiometric.py
│           │       ├── reflectance.py
│           │       └── level_1b.py
│           ├── pds4/           # PDS4 label generation
│           └── utils/          # Shared classes, error types, utilities
│
├── tests/                      # pytest test suite
├── notebooks/                  # Jupyter notebooks for exploration
└── docs/                       # Documentation and diagrams
```

---

## Input Data Format

The pipeline expects input data in a specific directory layout. The path to this directory is set via `run.input_dir` in the configuration file.

```text
<input_dir>/
│
├── meta/
│   ├── telemetry.json      # Instrument telemetry (temperatures, clocks, modes)
│   └── config.json         # Instrument configuration for the acquisition
│
└── acq_000/                # Acquisition directory (must start with "acq_")
    ├── dc_0_exp_000.bin    # NIR raw frame (binary, little-endian uint16, 518×648 px)
    ├── dc_0_exp_001.bin    # NIR raw frame (next exposure)
    ├── ...
    └── dc_1_exp_000.bin    # MIR raw value (4-byte big-endian uint32 per sample)
```

**Key points:**

- The `meta/` subdirectory and both JSON files are **required**. The pipeline raises a `ValidationError` if they are missing.
- The acquisition folder must match the pattern `acq_<digits>*`. If multiple matching folders exist the pipeline uses the lexicographically first one.
- NIR frames are raw 16-bit unsigned binary files (518 rows × 648 columns, little-endian). JPEG 2000 compressed files (`.bin.jp2`) are also supported if the decompressor binary has been compiled.
- MIR samples are 4-byte big-endian unsigned integers, one value per file. Channel IDs in filenames: `dc_0_*` = NIR, `dc_1_*` = MIR.

---

## Calibration Files

All calibration files must reside inside the directory specified by `calibration.calibration_dir`. The individual file paths in the config are **relative to that directory**.

| Config key | Format | Used in level | Description |
|---|---|---|---|
| `dark` | FITS, `PrimaryHDU` | 1B | Dark background frame subtracted from each exposure |
| `flat` | FITS, `PrimaryHDU` | 1B | Flat field frame for pixel-response normalisation |
| `badpixels` | Plain text `.txt` | 1B | List of bad pixel regions (see format below) |
| `nir_radiance` | Plain text `.txt` | 1B | Per-channel radiance conversion coefficients for NIR |
| `mir_radiance` | Plain text `.txt` | 1B | Per-channel radiance conversion coefficients for MIR |
| `solar_ssi` | CSV | 1C | Solar spectral irradiance (wavelength nm, W m⁻² nm⁻¹ at 1 AU) |

**Bad pixel file format** — each line specifies one bad region:

```
<type> <col> <row> <size_x> <size_y>
```

Where `type` is one of:

| Code | Region type |
|---|---|
| `P` | Single pixel |
| `H` | Horizontal cluster |
| `V` | Vertical cluster |
| `R` | Rectangular region |

Use `-` for coordinates not applicable to the region type.

---

## Configuration

The pipeline is controlled by a YAML configuration file. By default, when run from the project root, the pipeline searches for `configs/pipeline.yaml`. A custom path can be passed with `--config`.

Copy the annotated example as a starting point:

```bash
cp configs/pipeline.example.yaml configs/pipeline.yaml
```

### Full configuration reference

```yaml
# ── Run-time settings ─────────────────────────────────────────────────────────
run:
  # (Required) Path to input data directory.
  # Relative to this config file, or absolute.
  input_dir: "../data/my_run/NIR"

  # (Optional) Root output directory.
  # If empty, outputs/<missphas>/ is created in the current working directory.
  output_dir: ""

  # (Optional) Path to the SPICE meta-kernel (.tm).
  # If empty, SPICE header fields will be populated with 'UNK'.
  # An absolute path is recommended.
  spice_dir: ""

  # Whether to overwrite existing output files.
  overwrite: true

# ── Calibration files ─────────────────────────────────────────────────────────
calibration:
  # (Required for level 1) Directory containing all calibration files.
  # Relative to this config file, or absolute.
  calibration_dir: "../calibration"

  # Paths below are relative to calibration_dir.
  dark:          "DARKS/NIR_DARK.fits"
  flat:          "FLATS/NIR_FLAT.fits"
  badpixels:     "BADPIXELS/NIR_BADPIXELS.txt"
  nir_radiance:  "RADIANCE/NIR_RADIANCE.txt"
  mir_radiance:  "RADIANCE/MIR_RADIANCE.txt"
  solar_ssi:     "SOLAR/ssi_yearly_avg_e2024_c20250221.csv"

  # (Optional) Manually set the solar distance in AU for reflectance
  # calibration (level 1C). Overrides the SPICE-derived value when set.
  # Use a numeric string e.g. "1" or "1.2", or leave empty to use SPICE.
  solar_distance: ""

# ── Observation metadata ──────────────────────────────────────────────────────
data:
  instrume:  "NIRMIR"                 # Camera/instrument ID
  origin:    "ESA-COMET-INTERCEPTOR"  # Mission
  swcreate:  "NIRMIRCAL"              # Software identification string
  missphas:  "000_test"               # Mission phase ID (used in output dir name)
  observ:    "pipeline-testing"       # Observation ID
  object:    "test"                   # Human-readable target name
  target:    "TEST"                   # SPICE target body name

# ── Pipeline control ──────────────────────────────────────────────────────────
pipeline:
  # Processing levels to execute.
  # "1" is a shorthand for all level-1 steps: ["1A", "1B", "1C"]
  # Allowed values: "0", "1", "1A", "1A-extra", "1B", "1C"
  levels: ["0", "1"]

  # Instrument channels to process.
  # Allowed values: "NIR", "MIR"
  channels: ["NIR"]
```

### Notes on paths

All path fields (`input_dir`, `output_dir`, `spice_dir`, `calibration_dir`) may be:

- **Absolute** paths (recommended for `spice_dir`)
- **Relative** to the directory containing the config file

---

## Running the Pipeline

### Default execution

Run from the project root with `configs/pipeline.yaml` present:

```bash
mirmis run
```

### Custom configuration file

```bash
mirmis run --config path/to/my_config.yaml
```

The path may be absolute or relative to the current working directory.

### Visualise a FITS product

View a single FITS file:

```bash
mirmis view --path path/to/file.fits
```

View all products at a specific processing level inside an output directory:

```bash
mirmis view --path path/to/output_directory --level 1B
```

Allowed `--level` values: `0A`, `1A`, `1B`, `1C`, `2A`, `2B`.

---

## Processing Levels

The `levels` list in the configuration controls which levels are executed. Levels are processed in order; each level takes the output of the previous one as input.

| Level | Config value | Input file | Description |
|---|---|---|---|
| **0A** | `"0"` | Raw binary + JSON metadata | Reads raw binary frames, queries SPICE for geometry, assembles a FITS file with a fully populated header. Produces one `*_0A.fits` per channel. |
| **1A** | `"1A"` | `*_0A.fits` | Populates additional calibrated header fields: wavelengths per frame, instrument-specific tuning keywords. Produces `*_1A.fits`. |
| **1A-extra** | `"1A-extra"` | `*_1A.fits` | Optional intermediate product saved before radiometric calibration; calibrated instrument DNs. Produces `*_1A-extra.fits`. |
| **1B** | `"1B"` | `*_1A.fits` | Full radiometric calibration: CDS pixel extraction → dark subtraction → flat-field correction → bad pixel replacement → radiance conversion. Produces `*_1B.fits` in physical units (W m⁻² sr⁻¹ nm⁻¹). |
| **1C** | `"1C"` | `*_1B.fits` | Converts radiance to **I/f reflectance** using solar spectral irradiance and heliocentric distance. NIR FWHM = 30 nm; MIR FWHM = 40 nm. Produces `*_1C.fits`. NIR channel only — MIR stops at 1B. |

**Shorthand:** `"1"` in the levels list is equivalent to `["1A", "1B", "1C"]` and runs all level-1 steps in sequence.

### Chaining runs

If you run level 0 and level 1 in separate invocations, set `run.input_dir` in the second config to point to the output directory of the first run (the directory containing `*_0A.fits`). The pipeline resolves the correct input file by searching for a FITS file matching the expected level suffix and channel prefix.

---

## Outputs

By default the pipeline writes outputs to:

```
outputs/<missphas>/
```

relative to the working directory. This can be overridden with `run.output_dir` in the config.

### Output file naming

```
<CHANNEL>_<BASE32_SC_CLOCK>_<YYMMDDTHHMMSS>_<LEVEL>.fits
```

For example:

```
NIR_0ABC12_260115T123456_0A.fits
NIR_0ABC12_260115T123456_1A.fits
NIR_0ABC12_260115T123456_1B.fits
NIR_0ABC12_260115T123456_1C.fits
```

The spacecraft clock is encoded as a 6-character base-32 string derived from the SC clock counter, producing a compact, monotonically increasing image identifier.

### FITS file structure

Each output file contains a single `PrimaryHDU` with the image data array (frames × rows × columns for NIR; a 1-D array for MIR) and a populated header containing three groups of keywords:

**Acquisition metadata**

| Keyword | Description |
|---|---|
| `INSTRUME` | Instrument ID |
| `ORIGIN` | Mission |
| `MISSPHAS` | Mission phase ID |
| `OSERV_ID` | Observation ID |
| `FILENAME` | Output filename |
| `ORIGFILE` | Source raw filename |
| `SWCREATE` | Processing software ID |
| `DATE` | File creation timestamp (UTC) |
| `PROCLEVL` | Processing level of this file |
| `DATE_OBS` | Observation timestamp (UTC) |
| `SC_CLK` | Spacecraft clock at acquisition |
| `OBJECT` | Target name |

**SPICE geometry** (populated when `spice_dir` is provided; otherwise `UNK`)

| Keyword | Description |
|---|---|
| `SPICE_MK` | SPICE meta-kernel used |
| `SPICEVER` | SPICE dataset version |
| `SUN_POSX/Y/Z` | Sun position vector (km) |
| `SOLAR_D` | Heliocentric distance (AU) |
| `EARTPOSX/Y/Z` | Earth position vector (km) |
| `EARTH_D` | Geocentric distance (AU) |
| `TARGET` | Observation target |
| `TRG_POSX/Y/Z` | Target position vector (km) |
| `TRG_DIST` | Distance to target (km) |
| `SC_QUATW/X/Y/Z` | Spacecraft attitude quaternion |
| `CAM_RA/DEC` | Camera boresight (RA/Dec, deg) |
| `CAM_NAZ` | Camera nadir azimuth (deg) |
| `SOL_ELNG` | Solar elongation (deg) |

**Instrument-specific metadata** (stored as `HIERARCH` keywords)

| Keyword | Description |
|---|---|
| `CHANNELS` | Active channel (`NIR` or `MIR`) |
| `NIR_CCDTEMP` | NIR detector temperature |
| `NIR_FPI_TEMP1/2` | NIR Fabry-Pérot etalon temperatures |
| `MIR_CCDTEMP` | MIR detector temperature |
| `MIR_FPI_TEMP1/2` | MIR Fabry-Pérot etalon temperatures |
| `<CH>_FRAMES` | Comma-separated list of frame indices |
| `<CH>_TASK_<N>` | setpoints and exposures from taskfile |
| `<CH>_WL_<N>` | Tuned wavelength (nm) for frame N |
| `<CH>_EXP_<N>` | Exposure in seconds for frame N |

---

## Additional Resources

| Resource | Location |
|---|---|
| Annotated example config | [`configs/pipeline.example.yaml`](configs/pipeline.example.yaml) |
| Additional documentation | [`docs/`](docs/) |
| CI MIRMIS NIR MIR Data Pipeline PDF | [`docs/CI_MIRMIS_NIR_MIR_Data_Pipeline.pdf`](docs/CI_MIRMIS_NIR_MIR_Data_Pipeline.pdf) |
| Unit tests | [`tests/`](tests/) |
| Jasper library (JP2 decompression) | https://jasper-software.github.io/jasper-manual/latest/html/index.html |
| SpiceyPy documentation | https://spiceypy.readthedocs.io |
| Comet Interceptor mission | https://www.cosmos.esa.int/web/comet-interceptor |

---

## Contact

Maintained by **Valtteri Pitkänen**  
GitHub: https://github.com/Valtterimj  
Email: valtteri.m.pitkanen@aalto.fi | valtterimj@gmail.com
