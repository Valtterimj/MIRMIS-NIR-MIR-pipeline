
from pathlib import Path
from astropy.io import fits
from astropy.io.fits import Header

from nirmir_pipeline.pipeline.utils.utilities import channel_to_id, id_to_channel
from nirmir_pipeline.pipeline.utils.errors import PipelineError
from nirmir_pipeline.pipeline.utils.classes import InputLayout, Config, Issue, HeaderEntry
from nirmir_pipeline.pipeline.levels.level_0.metadata import collect_metadata

def add_entries_to_header(hdr: Header, entries: dict[str, HeaderEntry], hierarch: bool = False) -> None:
    for key, entry in entries.items():
        k = f"HIERARCH {key}" if hierarch else key

        value = "" if entry.value is None else str(entry.value)
        comment = None if entry.comment is None else str(entry.comment)

        if not comment:
            hdr[k] = value
            continue

        card_length = len(k) + len(value) + len(comment) + 4
        if card_length <= 80:
            hdr[k] = (value, comment)
        else: 
            hdr[k] = (value, "")

            

def build_fits(input: InputLayout, cfg: Config, channel: str) -> Path:
    """
    Creates a new level_0 FITS file by combining releavnt metadata with the acquisitions.
    
    :param input: object containing the paths to the input directories (meta, acq_)
    :type input: InputLayout
    :param cfg: Pipeline configuration object
    :type cfg: Config
    :param channel: Instrument channel
    :type channel: String
    :return: path to the created FITS file. 
    :rtype: Path
    """

    all_issues: list[Issue] = []

    # Primary HDU
    primary_hdu = fits.PrimaryHDU()
    primary_header = primary_hdu.header

    meta_data, meta_issues = collect_metadata(input=input, spice=cfg.run.spice_dir, cfg=cfg.data, channel=channel)
    all_issues.extend(meta_issues)

    acq_meta = meta_data.acq
    add_entries_to_header(hdr=primary_header, entries=acq_meta, hierarch=False)
    spice_meta = meta_data.spice
    add_entries_to_header(hdr=primary_header, entries=spice_meta, hierarch=False)
    inst_meta = meta_data.instrument
    add_entries_to_header(hdr=primary_header, entries=inst_meta, hierarch=False)
    inst_s_meta = meta_data.instrument_specific
    add_entries_to_header(hdr=primary_header, entries=inst_s_meta, hierarch=True)

    

    raise NotImplementedError