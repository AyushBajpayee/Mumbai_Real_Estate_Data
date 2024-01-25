import pandas as pd
import time
import os
import urllib.request
import ssl
ssl._create_default_https_context = ssl._create_unverified_context

import warnings
warnings.filterwarnings("ignore")

from tqdm import tqdm
import pdfplumber

import subprocess
import pathlib
import ghostscript as gs


def curves_to_edges(cs):
    edges = []
    for c in cs:
        edges += pdfplumber.utils.rect_to_edges(c)
    return edges

# Get the bounding boxes of the tables on the page.
def not_within_bboxes(obj):
    """Check if the object is in any of the table's bbox."""
    def obj_in_bbox(_bbox):
        """See https://github.com/jsvine/pdfplumber/blob/stable/pdfplumber/table.py#L404"""
        v_mid = (obj["top"] + obj["bottom"]) / 2
        h_mid = (obj["x0"] + obj["x1"]) / 2
        x0, top, x1, bottom = _bbox
        return (h_mid >= x0) and (h_mid < x1) and (v_mid >= top) and (v_mid < bottom)
    return not any(obj_in_bbox(__bbox) for __bbox in bboxes)

out_folder = "out"
pdf_file = pd.read_csv("pdf_urls.csv")


cols = ["sl_no", "pdf_urls", "save_path", "non_table_text", "table_dict"]
extracted_df = pd.DataFrame(columns = cols)

index = 1
for url in tqdm(pdf_file["pdf_urls"].to_list()):

    curr_file_path = pathlib.Path(__file__).parent.resolve()
    os.makedirs("out", exist_ok = True)
    save_path = curr_file_path.joinpath(f"out/{index}.pdf")

    urllib.request.urlretrieve(url, save_path)

    time.sleep(2)
    pdf = pdfplumber.open(save_path)
    page = pdf.pages[0]


    table_settings = {
        "vertical_strategy": "explicit",
        "horizontal_strategy": "explicit",
        "explicit_vertical_lines": curves_to_edges(page.curves + page.edges),
        "explicit_horizontal_lines": curves_to_edges(page.curves + page.edges),
        "intersection_y_tolerance": 10,
    }

    bboxes = [table.bbox for table in page.find_tables(table_settings=table_settings)]

    non_table_data = page.filter(not_within_bboxes).extract_text()

    table_data_in_nested_list = page.extract_table()
    table_data = {li[0]: li[1] for li in table_data_in_nested_list}

    curr_df = pd.DataFrame([[index, url, save_path, non_table_data, table_data]], columns = cols)
    extracted_df = pd.concat([extracted_df, curr_df],ignore_index=True)

    index += 1
extracted_df = extracted_df.reset_index(drop=True)

# use extracted_df here.
