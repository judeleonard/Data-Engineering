from pprint import pprint
import pandas as pd
import json
import os

project_root = "/usr/local/airflow"


def organize_info_data(columns: list, key: str):
    file_list = os.listdir(f"{project_root}/include/dataset/odis_json/")
    data = pd.DataFrame(columns=columns)
    for file in file_list:
        with open(os.path.join(f"{project_root}/include/dataset/odis_json/", file), "r", encoding="utf-8") as f:
            try:
                df = json.load(f)
                data.loc[len(data)] = df[key]
            except json.decoder.JSONDecodeError:
                print("The string does NOT contain valid JSON")
    return data


def preprocess_infos(match_result_info):
    """
    preprocesses info further to have clean the outcome column
    """
    normalize_match_result = pd.json_normalize(match_result_info["outcome"])
    preprocessed_match_result = pd.concat(
        [match_result_info[["dates"]], normalize_match_result], axis=1
    )
    preprocessed_match_result

    # merge the two final result data preprocessed_match_result and match_result and drop the unwanted columns
    merged_df = pd.merge(match_result_info, preprocessed_match_result, on="dates")

    # final data cleaning...
    # Rename columns by replacing "." with "_"
    final_match_result = merged_df.rename(columns=lambda x: x.replace(".", "_"))

    # drop the useless outcome column
    final_match_result.drop(
        "outcome", axis=1, inplace=True
    )  # we can load this into the db as **"dim_match_result"**

    return export_transformed_data(
        data=final_match_result, filename="match_results.csv"
    )


def export_transformed_data(data, filename, path=f"{project_root}/include/dataset/transformed"):
    fname = os.path.join(path, filename)
    return data.to_csv(fname, index=False)
