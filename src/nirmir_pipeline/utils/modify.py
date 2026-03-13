from astropy.io import fits
from astropy.io.fits import HDUList, Header, PrimaryHDU
from pathlib import Path
import numbers


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


def main() -> None:
    # fill the fits file path here
    fits_file = repo_root / 'tests' / 'data' / 'fits' / 'MIR_1A.fits'

    # Open the fits file
    with fits.open(fits_file, mode="update") as hdul:
        primary = hdul[0]
        header = primary.header
        data = primary.data

        print_header(header)
        print(data)



if __name__ == "__main__":
    main()