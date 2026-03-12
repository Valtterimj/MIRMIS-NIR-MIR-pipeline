
from astropy.io import fits
from pathlib import Path
import matplotlib
matplotlib.use("TkAgg")
import matplotlib.pyplot as plt
import numpy as np


def visualise_fits(file: str | Path, cmap: str ='gray') -> None:

    file = Path(file)

    with fits.open(file) as hdul:
        data = hdul[0].data
        header = hdul[0].header
    
    print(repr(header))
    
    if data is None:
        raise ValueError(f"{file.name}: Primary HDU has no data")
    
    if data.ndim == 1:
        print('Data is a one dimentional array')
        
        print(f"Length      : {len(data)}")
        print(f"Dtype       : {data.dtype}")
        print(f"Min         : {np.nanmin(data):.4g}")
        print(f"Mean        : {np.nanmean(data):.4g}")
        print(f"Max         : {np.nanmax(data):.4g}")
        print(f"\nArray values: ")
        print(data)
        return
    
    if data.ndim == 2:
        print(f"Dtype       : {data.dtype}")
        print(f"Min         : {np.nanmin(data):.4g}")
        print(f"Mean        : {np.nanmean(data):.4g}")
        print(f"Max         : {np.nanmax(data):.4g}")
        plt.figure()
        plt.imshow(data, cmap=cmap)
        plt.title(file.name)
        plt.show()
        return
    
    if data.ndim != 3: 
        raise ValueError(f"{file.name}: expected 1D, 2D, or 3D data, got shape={data.shape}")
    
    print(f"Dtype       : {data.dtype}")
    n_frames = data.shape[0]

    fig, ax = plt.subplots()
    plt.subplots_adjust(bottom=0.15)

    idx = 0
    im = ax.imshow(data[idx], cmap=cmap)
    title = ax.set_title(f"Frame {idx+1} /{n_frames}")

    def on_key(event):
        nonlocal idx
        if event.key in ["right", "d"]:
            idx = (idx + 1) % n_frames
        elif event.key in ["left", "a"]:
            idx = (idx - 1) % n_frames
        else:
            return

        frame = data[idx]
        min = round(float(np.min(frame)), 2)
        mean = round(float(np.mean(frame)), 2)
        max = round(float(np.max(frame)),2)
        print(f"Frame {idx + 1}: min, mean, max: {min}, {mean}, {max}")

        im.set_data(data[idx])
        title.set_text(f"{file.name} | Frame {idx+1}/{n_frames}")
        fig.canvas.draw_idle()
    
    fig.canvas.mpl_connect("key_press_event", on_key)

    print("Use left/right arrow keys or 'a'/'d' to move between frames.")
    plt.show()