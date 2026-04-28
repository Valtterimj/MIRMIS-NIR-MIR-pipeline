import os
import sys
from astropy.io import fits
from jinja2 import Template
import datetime
import pandas as pd
from pathlib import Path

def find_acquisition_start(csv_path, acquisition_end_utc):
    df = pd.read_csv(csv_path)
    df['Time'] = pd.to_datetime(df['Time'], utc=True)
    df = df.sort_values('Time')
    acquisition_end = pd.to_datetime(acquisition_end_utc, utc=True)
    df = df[df['Time'] <= acquisition_end]
    
    # Identify where ACQUIRING state changes (start points)
    df['PrevMode'] = df['MilaAspectDpuMode'].shift(1)
    acquiring_starts = df[
        (df['MilaAspectDpuMode'] == 'ACQUIRING') &
        (df['PrevMode'] != 'ACQUIRING')
    ]
    
    # Find latest acquisition start before end time
    if acquiring_starts.empty:
        return None
    else:
        latest_start_time = acquiring_starts['Time'].iloc[-1]
        return latest_start_time.strftime("%Y-%m-%dT%H:%M:%S.%f")[:-3]
    
# print(find_acquisition_start('.../ASPECT state-data-2025-08-22 18_29_07.csv', '2025-08-22T16:34:58.000'))

def generate_pds4_label(fits_file_path, aspect_state_data, xml_output_folder):

    # This section or list of required keywords prints the missing keywords, and is solely for troubleshooting purposes. These are not directly reguired to generate the PDS4 label.
    required_keywords = {
        ('DATE-OBS',): 'Acquisition UTC date',
        ('NAXIS1',): 'Image width',
        ('NAXIS2',): 'Image height',
        ('NAXIS3',): 'Number of images',
        ('BITPIX',): 'Array data type',
        ('PROCLEVL',): 'Processing level',
        ('MISSPHAS',): 'Mission phase',
        ('OBJECT',): 'Target',
        ('SC_CLK',): 'SC clock Hera instrument format: 13480572:349872',
        ('SPICE_MK',): 'SPICE meta kernel version',
        ('DATE',): 'UTC time of file creation',
        ('HIERARCH ASP_CHANNELS',): 'Channels in this file'
    }

    # Read FITS header
    with fits.open(fits_file_path) as hdul:
        hdr = hdul[0].header

        # Warning check
    warnings = []
    for key_group, description in required_keywords.items():
        if not any(k in hdr for k in key_group):
            warnings.append(f"Missing: {', '.join(key_group)} ({description})")

    # Print warnings
    if warnings:
        print("Warnings:")

    for w in warnings:
        print(w)

    header_cards = len(hdr.cards)
    # Each card 80 characters =  80 bytes
    PrimaryHDU_size = ((header_cards * 80 + 2879) // 2880) * 2880
    PrimaryHDU_offset = 0

    # This section reads the listed items from the fits header. If there are changes to the header, update this section and troubleshoot any possible issues.
    naxis1 = hdr.get('NAXIS1', 0)
    naxis2 = hdr.get('NAXIS2', 0)
    naxis3 = hdr.get('NAXIS3', 0)
    date_obs = hdr.get('DATE-OBS', '0000-00-00T00:00:00.000')
    vis_exposure = hdr.get('0_EXPOS', 'UNKNOWN')
    nir1_exposure = hdr.get('1_EXPOS', 'UNKNOWN')
    nir2_exposure = hdr.get('2_EXPOS', 'UNKNOWN')
    swir_exposure = hdr.get('3_EXPOS', 'UNKNOWN')
    target = hdr.get('OBJECT', 'UNKNOWN')
    mission_phase = hdr.get('MISSPHAS', 'UNKNOWN')
    processing_level = hdr.get('PROCLEVL', 'UNKNOWN')
    bitpix = hdr.get('BITPIX', 'UNKNOWN')
    sc_clk = hdr.get('SC_CLK', 'UNKNOWN')
    spiceclk = hdr.get('SPICECLK', 'UNKNOWN')
    spice_mk = hdr.get('SPICE_MK', 'UNKNOWN')
    vis_ccdtemp = hdr.get('0_CCDTMP', 'UNKNOWN')
    nir1_ccdtemp = hdr.get('1_CCDTMP', 'UNKNOWN')
    nir2_ccdtemp = hdr.get('2_CCDTMP', 'UNKNOWN')
    swir_ccdtemp = hdr.get('3_CCDTMP', 'UNKNOWN')
    vis_fpi1 = hdr.get('0_FPI1', 'UNKNOWN')
    nir1_fpi1 = hdr.get('1_FPI1', 'UNKNOWN')
    nir2_fpi1 = hdr.get('2_FPI1', 'UNKNOWN')
    swir_fpi1 = hdr.get('3_FPI1', 'UNKNOWN')
    vis_fpi2 = hdr.get('0_FPI2', 'UNKNOWN')
    nir1_fpi2 = hdr.get('1_FPI2', 'UNKNOWN')
    nir2_fpi2 = hdr.get('2_FPI2', 'UNKNOWN')
    swir_fpi2 = hdr.get('3_FPI2', 'UNKNOWN')
    vis_frames = hdr.get('0_FRAMES', 'UNKNOWN')
    nir1_frames = hdr.get('1_FRAMES', 'UNKNOWN')
    nir2_frames = hdr.get('2_FRAMES', 'UNKNOWN')
    swir_frames = hdr.get('3_FRAMES', 'UNKNOWN')
    vis_order = hdr.get('0_ORDER', 'UNKNOWN')
    nir1_order = hdr.get('1_ORDER', 'UNKNOWN')
    nir2_order = hdr.get('2_ORDER', 'UNKNOWN')
    swir_order = hdr.get('3_ORDER', 'UNKNOWN')
    nir1_sp1 = hdr.get('1_SP1', 'UNKNOWN')
    nir1_sp2 = hdr.get('1_SP2', 'UNKNOWN')
    nir1_sp3 = hdr.get('1_SP3', 'UNKNOWN')
    nir2_sp1 = hdr.get('2_SP1', 'UNKNOWN')
    nir2_sp2 = hdr.get('2_SP2', 'UNKNOWN')
    nir2_sp3 = hdr.get('2_SP3', 'UNKNOWN')
    vis_wl = hdr.get('0_WL', 'UNKNOWN')
    nir1_wl = hdr.get('1_WL', 'UNKNOWN')
    nir2_wl = hdr.get('2_WL', 'UNKNOWN')
    swir_wl = hdr.get('3_WL', 'UNKNOWN')
    creation_time = hdr.get('DATE', 'UNKNOWN')
    channels = hdr.get('HIERARCH ASP_CHANNELS', 'UNKNOWN')#channels = hdr.get('CHANNELS', 'UNKNOWN')
    channels = channels.upper()
        
    # Basic metadata
    filename = os.path.basename(fits_file_path)
    lid_base = os.path.splitext(filename)[0].lower()

    # Determine data type and bytes per pixel based on BITPIX
    bitpix_to_type = {
        8: ("UnsignedByte", 1),
        16: ("UnsignedMSB2", 2),
        -32: ("IEEE754MSBSingle", 4),
        -64: ("IEEE754MSBDouble", 8)
    }

    data_type, bytes_per_pixel = bitpix_to_type.get(bitpix, ("UNKNOWN", None))

    # Compute object length
    if bytes_per_pixel is not None:
        object_length = naxis1 * naxis2 * bytes_per_pixel  # in bytes
    else:
        object_length = -999  

    source_lid_vis = 'unknown'
    source_lid_nir1 = 'unknown'
    source_lid_nir2 = 'unknown'
    source_lid_swir = 'unknown'
    source_lid = 'unknown'

    # Processing level mapping
    if processing_level == '0A':
        if "SWIR" in channels:
            template_path = Path(__file__).parent / "label_templates" / "ASPECT-0A-swir-template.xml"
        else:
            template_path = Path(__file__).parent / "label_templates" / "ASPECT-0A-vis-nir-template.xml"
        level_suffix = 'raw'
        previous_level_suffix = ''
        level_description = "Raw"
        unit = 'Brightness'
        previous_lid_base = ''
        source_lid = f"urn:esa:psa:hera_milani_aspect:data_{previous_level_suffix}:{previous_lid_base}"
    elif processing_level == '1A':
        if "SWIR" in channels:
            template_path = Path(__file__).parent / "label_templates" / "ASPECT-1A-swir-template.xml"
        else:
            template_path = Path(__file__).parent / "label_templates" / "ASPECT-1A-vis-nir-template.xml"
        level_suffix = 'raw'
        previous_level_suffix = 'raw'
        level_description = "Raw"
        unit = 'Brightness'
        previous_lid_base = lid_base[:-2] + '0a'
        source_lid = f"urn:esa:psa:hera_milani_aspect:data_{previous_level_suffix}:{previous_lid_base}"
    elif processing_level == '1B':
        if "SWIR" in channels:
            template_path = Path(__file__).parent / "label_templates" / "ASPECT-1B-swir-template.xml"
        else:
            template_path = Path(__file__).parent / "label_templates" / "ASPECT-1B-vis-nir-template.xml"
        level_suffix = 'partially_processed'
        previous_level_suffix = 'raw'
        level_description = "Partially Processed"
        unit = 'Brightness' 
        previous_lid_base = lid_base[:-2] + '1a'
        source_lid = f"urn:esa:psa:hera_milani_aspect:data_{previous_level_suffix}:{previous_lid_base}"
    elif processing_level == '1C':
        if "SWIR" in channels:
            template_path = Path(__file__).parent / "label_templates" / "ASPECT-1C-swir-template.xml"
        else:
            template_path = Path(__file__).parent / "label_templates" / "ASPECT-1C-vis-nir-template.xml"
        level_suffix = 'partially_processed'
        previous_level_suffix = 'partially_processed'
        level_description = "Partially Processed"
        unit = 'nm'
        previous_lid_base = lid_base[:-2] + '1b'
        source_lid = f"urn:esa:psa:hera_milani_aspect:data_{previous_level_suffix}:{previous_lid_base}"
    elif processing_level == '2A':
        if "SWIR" in channels:
            template_path = Path(__file__).parent / "label_templates" / "ASPECT-2A-swir-template.xml"
        else:
            template_path = Path(__file__).parent / "label_templates" / "ASPECT-2A-vis-nir-template.xml"
        level_suffix = 'partially_processed'
        previous_level_suffix = 'partially_processed'
        level_description = "Partially Processed"
        unit = 'nm'
        previous_lid_base = lid_base[:-2] + '1c'
        source_lid = f"urn:esa:psa:hera_milani_aspect:data_{previous_level_suffix}:{previous_lid_base}"
    elif processing_level == '2B':
        template_path = Path(__file__).parent / "label_templates" / "ASPECT-2B-template.xml"
        level_suffix = 'calibrated'
        previous_level_suffix = 'partially_processed'
        level_description = "Calibrated"
        unit = 'nm'
        previous_lid_base_vis = lid_base[:2] + '0' + lid_base[3:-2] + '1c'
        previous_lid_base_nir1 = lid_base[:2] + '1' + lid_base[3:-2] + '1c'
        previous_lid_base_nir2 = lid_base[:2] + '2' + lid_base[3:-2] + '1c'
        previous_lid_base_swir = lid_base[:2] + '3' + lid_base[3:-2] + '1c'
        source_lid_vis = f"urn:esa:psa:hera_milani_aspect:data_{previous_level_suffix}:{previous_lid_base_vis}"
        source_lid_nir1 = f"urn:esa:psa:hera_milani_aspect:data_{previous_level_suffix}:{previous_lid_base_nir1}"
        source_lid_nir2 = f"urn:esa:psa:hera_milani_aspect:data_{previous_level_suffix}:{previous_lid_base_nir2}"
        source_lid_swir = f"urn:esa:psa:hera_milani_aspect:data_{previous_level_suffix}:{previous_lid_base_swir}"
    else:
        level_suffix = 'unknown'
        level_description = 'Unknown'
        unit = 'UNKNOWN'
        source_lid = 'unknown'

    lid = f"urn:esa:psa:hera_milani_aspect:data_{level_suffix}:{lid_base}"
    title = f"{processing_level} {level_description} datacube {filename} from the Aspect hyperspectral imager on Milani in the Hera Mission"
    fits_header = f"Fits header. Contains metadata about the datacube."

    try:
        start_time = find_acquisition_start(aspect_state_data, date_obs)
        t1 = datetime.datetime.fromisoformat(start_time)
        t2 = datetime.datetime.fromisoformat(date_obs)
        time_delta = t2 - t1
        time_delta = time_delta.total_seconds()
        start_time = start_time + "Z"
    except Exception:
        print(f"Exception: {sys.exc_info()[0]} -> Setting acquisition start time to UNKNOWN.")
        print("ASPECT state-data file may be missing or mismatching with the fits.")
        start_time = 'UNKNOWN'

    # Determine target type
    if target.lower() == 'didymos':
        target_type = 'Asteroid'
        target_lid_reference = 'urn:nasa:pds:context:target:asteroid.65803_didymos'
    elif target.lower() == 'dimorphos':
        target_type = 'Asteroid' # must be equal to one of the following values: 'Asteroid', 'Astrophysical', 'Calibration', 'Calibration Field', 'Calibrator', 'Centaur', 'Comet', 'Dust', 'Dwarf Planet', 'Equipment', 'Exoplanet System', 'Galaxy', 'Globular Cluster', 'Laboratory Analog', 'Lunar Sample', 'Magnetic Field', 'Meteorite', 'Meteoroid', 'Meteoroid Stream', 'Nebula', 'Open Cluster', 'Planet', 'Planetary Nebula', 'Planetary System', 'Plasma Cloud', 'Plasma Stream', 'Ring', 'Sample', 'Satellite', 'Sky', 'Star', 'Star Cluster', 'Synthetic Sample', 'Terrestrial Sample', 'Trans-Neptunian Object'
        target_lid_reference = 'UNKNOWN'
    elif target.lower() in ['DARK']:
        target_type = 'Calibration'
        target_lid_reference = 'UNKNOWN'
    else:
        target_type = 'UNKNOWN'
        target_lid_reference = 'UNKNOWN'

    if all(channel in channels for channel in ["VIS", "NIR1", "NIR2", "SWIR"]):
        wavelengths = f'({vis_wl},{nir1_wl},{nir2_wl},{swir_wl})'
        all_exposure_times = f'({vis_exposure}, {nir1_exposure}, {nir2_exposure}, {swir_exposure})'
        lowest_wl = vis_wl.split(',')[0]
        fov_description = 'VIS, NIR1, and NIR2 channels have a rectangular FoV. SWIR channel has a circular FoV.'
        fov_width = '5.4'
        fov_length = '6.7'
        wavelength_range = 'Near Infrared'
        try:
            net_integration_time = len(vis_wl.split(','))*float(vis_exposure) + len(nir1_wl.split(','))*float(nir1_exposure) + len(nir2_wl.split(','))*float(nir2_exposure) + len(swir_wl.split(','))*float(swir_exposure)
            net_integration_time = str(net_integration_time)
        except:
            net_integration_time = 'UNKNOWN'
    elif all(channel in channels for channel in ["VIS", "NIR1", "NIR2"]):
        wavelengths = f'({vis_wl},{nir1_wl},{nir2_wl})'
        all_exposure_times = f'({vis_exposure}, {nir1_exposure}, {nir2_exposure})'
        lowest_wl = vis_wl.split(',')[0]
        fov_description = 'VIS, NIR1, and NIR2 channels have a rectangular FoV.'
        fov_width = '5.4'
        fov_length = '6.7'
        wavelength_range = 'Near Infrared'
        try:
            net_integration_time = len(vis_wl.split(','))*float(vis_exposure) + len(nir1_wl.split(','))*float(nir1_exposure) + len(nir2_wl.split(','))*float(nir2_exposure)
            net_integration_time = str(net_integration_time)
        except:
            net_integration_time = 'UNKNOWN'
    elif all(channel in channels for channel in ["NIR1", "NIR2"]):
        wavelengths = f'({nir1_wl},{nir2_wl})'
        all_exposure_times = f'({nir1_exposure}, {nir2_exposure})'
        lowest_wl = nir1_wl.split(',')[0]
        fov_description = 'NIR1, and NIR2 channels have a rectangular FoV.'
        fov_width = '5.4'
        fov_length = '6.7'
        wavelength_range = 'Near Infrared'
        try:
            net_integration_time = len(nir1_wl.split(','))*float(nir1_exposure) + len(nir2_wl.split(','))*float(nir2_exposure)
            net_integration_time = str(net_integration_time)
        except:
            net_integration_time = 'UNKNOWN'
    elif "VIS" in channels:
        wavelengths = f'({vis_wl})'
        all_exposure_times = vis_exposure
        lowest_wl = vis_wl.split(',')[0]
        fov_description = 'VIS has a rectangular FoV.'
        fov_width = '10'
        fov_length = '10'
        wavelength_range = 'Visible'
        try:
            net_integration_time = len(vis_wl.split(','))*float(vis_exposure)
        except:
            net_integration_time = 'UNKNOWN'
    elif "NIR1" in channels:
        wavelengths = f'({nir1_wl})'
        all_exposure_times = nir1_exposure
        lowest_wl = nir1_wl.split(',')[0]
        fov_description = 'NIR1 has a rectangular FoV.'
        fov_width = '5.4'
        fov_length = '6.7'
        wavelength_range = 'Near Infrared'
        try:
            net_integration_time = len(nir1_wl.split(','))*float(nir1_exposure)
            net_integration_time = str(net_integration_time)
        except:
            net_integration_time = 'UNKNOWN'
    elif "NIR2" in channels:
        wavelengths = f'({nir2_wl})'
        all_exposure_times = nir2_exposure
        lowest_wl = nir2_wl.split(',')[0]
        fov_description = 'NIR2 has a rectangular FoV.'
        fov_width = '5.4'
        fov_length = '6.7'
        wavelength_range = 'Near Infrared'
        try:
            net_integration_time = len(nir2_wl.split(','))*float(nir2_exposure)
            net_integration_time = str(net_integration_time)
        except:
            net_integration_time = 'UNKNOWN'
    elif "SWIR" in channels:
        wavelengths = f'({swir_wl})'
        all_exposure_times = swir_exposure
        lowest_wl = swir_wl.split(',')[0]
        fov_description = 'SWIR has a circular FoV.'
        fov_width = '5.85'
        fov_length = '5.85'
        wavelength_range = 'Near Infrared'
        try:
            net_integration_time = len(swir_wl.split(','))*float(swir_exposure)
        except:
            net_integration_time = 'UNKNOWN'

    # compute spacecraft_clock_end_count (hypothesis: 1 000 000 ticks/second, change if 65536 ticks/second)
    try:
        sec_str, frac_str = sc_clk.split(":")
        sec = int(sec_str)
        frac = int(frac_str)
        total_seconds = sec + frac / 1000000
        total_seconds -= time_delta
        sec_int = int(total_seconds)
        frac_int = round((total_seconds - sec_int) * 1000000)
        if frac_int >= 1000000:
            sec_int += 1
            frac_int -= 1000000
        sc_clk_delta_sum = f"{sec_int}:{frac_int:06d}"
    except:
        sc_clk_delta_sum = sc_clk

    modification_date = datetime.datetime.now(datetime.timezone.utc).strftime("%Y-%m-%d")

    # Prepare template context
    context = {
        'lid': lid,
        'title': title,
        'filename': filename,
        'local_id': lid_base,
        'naxis1': naxis1,
        'naxis2': naxis2,
        'naxis3': naxis3,
        'start_time': start_time,
        'stop_time': date_obs if date_obs.endswith('Z') else date_obs + 'Z',
        'vis_exposure_time': vis_exposure,
        'nir1_exposure_time': nir1_exposure,
        'nir2_exposure_time': nir2_exposure,
        'swir_exposure_time': swir_exposure,
        'all_exposure_times': all_exposure_times,
        'mission_phase': mission_phase,
        'target_name': target,
        'target_type': target_type,
        'data_type': data_type,
        'object_length': object_length,
        'PrimaryHDU_offset': PrimaryHDU_offset,
        'PrimaryHDU_size': PrimaryHDU_size,
        'fits_header': fits_header,
        'unit': unit,
        'spacecraft_clock_start_count': sc_clk_delta_sum,
        'spacecraft_clock_end_count': sc_clk,
        'spice_mk':spice_mk,
        'lowest_wl': lowest_wl,
        'wavelengths': wavelengths,
        'creation_time': creation_time,
        'fov_description': fov_description,
        'fov_width': fov_width,
        'fov_length': fov_length,
        'net_integration_time': net_integration_time,
        'modification_date': modification_date,
        'processing_level': level_description,
        'wavelength_range': wavelength_range,
        'target_lid_reference': target_lid_reference,
        'source_lid': source_lid,
        'source_lid_vis': source_lid_vis,
        'source_lid_nir1': source_lid_nir1,
        'source_lid_nir2': source_lid_nir2,
        'source_lid_swir': source_lid_swir,
    }

    # Load and render XML template
    with open(template_path, 'r') as f:
        template = Template(f.read())
    label = template.render(context)

    # Write rendered label to file
    label_name = filename[:-5].lower()
    output_xml_path = os.path.join(xml_output_folder, label_name) + '.lblx'
    with open(output_xml_path, 'w') as f:
        f.write(label)

   # Print confirmation message
    print("XML label created successfully:")
    print(f"Saved to: {output_xml_path}")


test = False
if test:
    fits_file_path = '' # For example ASP_000000_200101T015217_2B.fits
    aspect_state_data = Path(__file__).parent / "ASPECT state-data-2025-08-22 18_29_07.csv"
    xml_output_folder = ''
    generate_pds4_label(fits_file_path, aspect_state_data, xml_output_folder)