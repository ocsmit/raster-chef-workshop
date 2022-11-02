#!/usr/bin/env python3

from pystac_client import Client
from collections import defaultdict
import json
import pandas as pd
import geopandas as gpd
import datetime
from collections import namedtuple
from osgeo import gdal
import rasterio
from rasterio.features import bounds
from pyproj import Transformer
import numpy as np
import os


from typing import Dict


# Set up VSIcurl settings
gdal.SetConfigOption("GDAL_HTTP_COOKIEFILE", "~/cookies.txt")
gdal.SetConfigOption("GDAL_HTTP_COOKIEJAR", "~/cookies.txt")
gdal.SetConfigOption("GDAL_DISABLE_READDIR_ON_OPEN", "YES")
gdal.SetConfigOption("CPL_VSIL_CURL_ALLOWED_EXTENSIONS", "TIF")


def downloader(item, geometry, outpath):
    for band, file_dict in item["assets"].items():
        fp = file_dict["href"]
        with rasterio.open(fp) as geo_fp:
            bbox = bounds(geometry)
            coord_transformer = Transformer.from_crs("epsg:4326", geo_fp.crs)
            # calculate pixels to be streamed in cog
            coord_upper_left = coord_transformer.transform(bbox[3], bbox[0])
            coord_lower_right = coord_transformer.transform(bbox[1], bbox[2])
            pixel_upper_left = geo_fp.index(coord_upper_left[0], coord_upper_left[1])
            pixel_lower_right = geo_fp.index(coord_lower_right[0], coord_lower_right[1])

            for pixel in pixel_upper_left + pixel_lower_right:
                # If the pixel value is below 0, that means that
                # the bounds are not inside of our available dataset.
                if pixel < 0:
                    print("Provided geometry extends available datafile.")
                    print("Provide a smaller area of interest to get a result.")
                    exit()

            # make http range request only for bytes in window
            window = rasterio.windows.Window.from_slices(
                (pixel_upper_left[0], pixel_lower_right[0]),
                (pixel_upper_left[1], pixel_lower_right[1]),
            )
            subset = geo_fp.read(1, window=window)
            if outpath is not None:
                height, width = subset.shape

                profile = geo_fp.profile.copy()
                profile["width"] = width
                profile["height"] = height
                transform = profile["transform"]
                new_transform = rasterio.Affine(
                    transform[0],
                    transform[1],
                    coord_upper_left[0],
                    transform[3],
                    transform[4],
                    coord_upper_left[1],
                )
                profile["transform"] = new_transform
                print(f"Writing {os.path.join(outpath, fp.split(r'/')[-1])}")
                with rasterio.open(
                    os.path.join(outpath, fp.split("/")[-1]), "w", **profile
                ) as dst:
                    dst.write(subset, 1)


if __name__ == "__main__":
    import multiprocessing as mp

    STAC_URL = "https://cmr.earthdata.nasa.gov/stac"
    GEOJSON_PATH = "./data/farm.geojson"
    COLLECTIONS = ["HLSL30.v2.0", "HLSS30.v2.0"]
    date_range = "2022-06/2022-06"

    with open(GEOJSON_PATH, "r") as fp:
        geojson = json.load(fp)

    bbox = geojson["features"][0]["geometry"]
    print(json.dumps(bbox, indent=2))

    catalog = Client.open(f"{STAC_URL}/LPCLOUD/")

    search = catalog.search(
        collections=COLLECTIONS, intersects=bbox, datetime=date_range, limit=100
    )
    print(search.matched())

    item_collection = search.get_all_items()

    items = item_collection.to_dict()["features"]

    geometry = geojson
    outpath = "./data_out"

    pool = mp.Pool(5)
    results = pool.starmap(downloader, [(item, geojson, outpath) for item in items])
