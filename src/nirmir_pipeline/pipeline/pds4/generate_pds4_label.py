from jinja2 import Environment, FileSystemLoader
from pathlib import Path

from nirmir_pipeline.pipeline.pds4.fits_reader import read_fits_metadata
from nirmir_pipeline.pipeline.utils.errors import PipelineError
from nirmir_pipeline.pipeline.utils.utilities import convert_to_zulu_time, convert_processing_levels


def generate_label(fits_path: Path, templates_dir: Path, output_path: Path) -> None:
    """
    Generate PDS4 label for a fits file and save it to the output path.
    """
    metadata = read_fits_metadata(fits_path)
    channel = metadata.get('channel')
    file_name = metadata.get('file_name')

    metadata["start_date_time"] = convert_to_zulu_time(metadata['start_date_time'])
    metadata['processing_level'] = convert_processing_levels(metadata['processing_level'])


    if channel not in ('NIR', 'MIR'):
        raise PipelineError(f"Channel should be 'NIR' or 'MIR', found: {channel}")
    template = f"CI_MIRMIS_{channel}_template.xml.j2"

    env = Environment(loader=FileSystemLoader(templates_dir))
    template = env.get_template(template)
    label_xml = template.render(**metadata)

    output_path = output_path / f"CI_MIRMIS_{file_name.split('.')[0]}.xml"

    output_path = Path(output_path)
    output_path.parent.mkdir(parents=True, exist_ok=True)
    output_path.write_text(label_xml, encoding="utf-8")
