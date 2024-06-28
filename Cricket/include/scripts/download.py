import os
import requests
from pathlib import Path
from zipfile import ZipFile



# define project root path
project_root = "/usr/local/airflow"

targets = [
    "https://cricsheet.org/downloads/odis_json.zip",
    # "https://cricsheet.org/downloads/odis_female_json.zip",
    # "https://cricsheet.org/downloads/odis_male_json.zip"
]


def download_target():
    print("Downloading Target...")
    for link in targets:
        response = requests.get(link)
        try:
            if not (
                response.status_code == 204
                and response.headers["content-type"]
                .strip()
                .startswith("application/json")
            ):
                filepath = os.path.join(f"{project_root}/include/dataset/", link.split("/")[-1])
                filename = Path(filepath).name

                # Download File
                with open(filepath, "wb") as file:
                    file.write(response.content)
                print(f"ZIP file downloaded successfully. {(filename)}")

                # Extract downloaded files
                with ZipFile(filepath, "r") as object:
                    object.extractall(f"{project_root}/include/dataset/{filename.split('.')[0]}")
                print(f"File extracted successfully. {(filename)}")
            else:
                print("Failed to download ZIP file.")
        except ValueError as e:
            return e


