from astropy.io import fits
from astropy.io.fits import HDUList, Header, PrimaryHDU
from pathlib import Path
import numbers
import numpy as np

# to run this file from root: python3 src/nirmir_pipeline/utils/modify.py

repo_root: Path = Path(__file__).parent.parent.parent.parent

def delete_header_field(header: Header, field: str) -> None:
    if field in header:
        del header[field]
    else:
        print(f"Field {field} not found in header.")

def modify_header_field(header: Header, field: str, value: tuple[str, str]) -> None:
    if field in header:
        header[field] = value
    else:
        print(f"Field {field} not found in header.")

def add_header_field(header: Header, field: str, value: tuple[str, str], index: None | int | str) -> None:
    """
    Add new field to header. If index None append to the end, if int insert it to the index, if string insert it after the index field.
    """
    if field in header:
        print(f"Field {field} already in header.")
    else:
        if index == None:
            header[field] = value
        elif isinstance(index, numbers.Number):
            header.insert(index, (field, value[0], value[1]))
        elif isinstance(index, str):
            header.insert(index, (field, value[0], value[1]), after=True)

def print_header(header: Header) -> None:
    print(repr(header))

def create_binary(path: Path, w: int, h: int, dtype= np.uint16):

    path = Path(path)
    data = np.ones((h, w), dtype=dtype)
    data.tofile(path)

    print(f'new bin file: {path}')

def create_fits(path: Path, header: Path | None, shape: tuple[int, int, int] | None, limit: int = 3, dtype: str ='uint16'):
    """
    create a fits file to path. if copying an existing header give the path header. Determine the shape or use copy the shape from header. Limit the number of frames
    """
    if header is not None:
        with fits.open(header) as hdr_hdul:
            hdr = hdr_hdul[0].header
            copy_shape = hdr_hdul[0].data.shape
            dtype= hdr_hdul[0].data.dtype
    else: 
        hdr = fits.Header()
        # if shape is None:
        #     raise(ValueError(f"header and shape cannot be none"))
        copy_shape = shape
    
    # if shape is None:
    #     copy_shape = (min(copy_shape[0], limit), copy_shape[1], copy_shape[2])
    
    # data = np.ones(copy_shape, dtype=dtype)
    data = np.full((512, 640), 0.5, dtype=np.float32)

    hdu = fits.PrimaryHDU()
    # hdu.header = hdr
    hdu.data = data
    hdul = fits.HDUList(hdu)

    hdul.writeto(path, overwrite=True)


def main() -> None:

    # fits_path = repo_root / 'tests' / 'data' / 'fits' / 'lvl_1B' / 'MIR_000000_1111111111111_1B.fits'
    fits_path = repo_root / 'tests' / 'data' / 'calib' / 'FLAT_05.fits'
    # copy_header = repo_root / 'outputs' / '000_test' / 'MIR_000000_200101T015157_1A.fits'

    create_fits(path=fits_path, header=None, shape=(1, 512, 640))

    

if __name__ == "__main__":
    # to run this file from root: python3 src/nirmir_pipeline/utils/modify.py
    main()