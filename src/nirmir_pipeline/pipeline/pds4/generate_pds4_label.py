import shutil

from jinja2 import Environment, FileSystemLoader
from pathlib import Path

from nirmir_pipeline.pipeline.pds4.fits_reader import read_fits_metadata
from nirmir_pipeline.pipeline.utils.errors import PipelineError
from nirmir_pipeline.pipeline.utils.utilities import convert_to_zulu_time, convert_processing_levels, get_wavelengths


def generate_label(fits_path: Path, templates_dir: Path, output_path: Path) -> None:
    """
    Generate PDS4 label for a fits file and save it to the output path.
    """
    metadata = read_fits_metadata(fits_path)
    channel = metadata.get('channel')
    channel_lower = channel.lower() if channel else ''
    file_name = metadata.get('file_name')
    stem = file_name.split('.')[0]
    proclevl = metadata['proclevl']

    metadata["start_date_time"] = convert_to_zulu_time(metadata['start_date_time'])
    metadata["processing_level"] = convert_processing_levels(proclevl)
    metadata["reference_list"] = proclevl != '0A'
    if channel not in ('NIR', 'MIR'):
        raise PipelineError(f"Channel should be 'NIR' or 'MIR', found: {channel}")
    template = f"CI_MIRMIS_NIRMIR_template.xml.j2"

    ### Identification Area ###
    title = f"Comet Interceptor MIRMIS instrument {channel} channel level {proclevl} processed datacube:{stem}"
    metadata["title"] = title

    ### Observation Area ###
    wl_range = 'Near Infrared'
    if channel == 'MIR':
        wl_range = 'Infrared'
    metadata['wl_range'] = wl_range

    ### Investigation Area ###
    # lid_reference for comet interception 
    #TODO: correct the right lid once comet interceptor dictionary is done.
    mission_lid = "urn:nasa:pds:context:investigation:mission.comet_interceptor"
    metadata['mission_lid'] = mission_lid

    ### Observing System ###
    #TODO: correct the right name and lids 
    observing_system_name = f'MIRMIS_{channel}'
    metadata['os_name'] = observing_system_name
    # Host
    mirmis_lid = "urn:nasa:pds:contect:instrument_host.spacecraft.mirmis"
    metadata['mirmis_lid'] = mirmis_lid
    # Instrument
    instrument_lid = f"urn:nasa:pds:contect:instrument:.mirmis.{channel_lower}"
    metadata['instrument_lid'] = instrument_lid

    ### Image dictionary ###
    radiometric_type = 'Spectral Radiance'
    if proclevl == '1C': 
        radiometric_type = 'Radiance Factor'
    metadata['radiometric_type'] = radiometric_type

    ### Spectral Dictionary ###
    # The net integration time for FPI is the full observation interval
    #TODO: figure how the 'full observation interval' is determined
    net_integration_time = 1
    metadata['net_integration_time'] = net_integration_time

    ### Spectral Characteristics ###
    # Bin description
    sampling_interval = 30
    metadata['sampling_interval'] = sampling_interval
    bin_width = 30
    metadata['bin_width'] = bin_width
    
    wavelengths = get_wavelengths(fits_file=fits_path)
    if wavelengths != None:
        first_center = wavelengths[0]
        last_center = wavelengths[-1]
        metadata['first_center'] = first_center
        metadata['last_center'] = last_center
    else:
        if channel == 'NIR':
            metadata['first_center'] = '900'
            metadata['last_center'] = '1700'
        else:
            metadata['first_center'] = '2500'
            metadata['last_center'] = '5000'
    
    env = Environment(loader=FileSystemLoader(templates_dir))
    template = env.get_template(template)
    label_xml = template.render(**metadata)
    
    product_dir = Path(output_path) / stem
    product_dir.mkdir(parents=True, exist_ok=True)

    (product_dir / f"CI_MIRMIS_{stem}.xml").write_text(label_xml, encoding="utf-8")
    shutil.copy2(fits_path, product_dir / fits_path.name)
    return 
