# Copyright (C) 2024 University of Bordeaux, CyVi Group & University of Bayreuth,
# Ecological Resource Technology & Anish Koyamparambath, Christoph Helbig, Thomas Schraml
# This file is part of geopolrisk-py library.
# geopolrisk-py is free software: you can redistribute it and/or modify
# it under the terms of the GNU General Public License as published by
# the Free Software Foundation, either version 3 of the License, or
# (at your option) any later version.
# geopolrisk-py is distributed in the hope that it will be useful,
# but WITHOUT ANY WARRANTY; without even the implied warranty of
# MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
# GNU General Public License for more details.
# You should have received a copy of the GNU General Public License
# along with geopolrisk-py.  If not, see <https://www.gnu.org/licenses/>.

import itertools
from tqdm import tqdm
import pandas as pd
from .database import logging
from geopolrisk.assessment.database import Database
from geopolrisk.assessment.utils import regions
from .core import *
from .utils import *
import yaml
from pathlib import Path

def load_ei_mapping():
    """
    Load ecoinvent mapping from the YAML file.
    """

    script_dir = Path(__file__).resolve().parent
    yaml_path = script_dir / "ecoinvent_mapping.yaml"
    with open(yaml_path, "r") as file:
        ecoinvent_mapping = yaml.safe_load(file)["ecoinvent_mappings"]
    return ecoinvent_mapping

def generate_create_table_sql(df: pd.DataFrame, table_name: str = "recordData") -> str:
    type_map = {
        "object": "TEXT",
        "int64": "INTEGER",
        "float64": "REAL",
        "bool": "INTEGER"
    }
    columns = []
    for col, dtype in df.dtypes.items():
        coltype = type_map.get(str(dtype), "TEXT")
        if col == "DBID":
            columns.append(f"{col} TEXT PRIMARY KEY")
        else:
            columns.append(f'"{col}" {coltype}')
    columns_clause = ",\n    ".join(columns)
    return f"CREATE TABLE IF NOT EXISTS {table_name} (\n    {columns_clause}\n);"


def gprs_calc(period: list, countries: list, resources: list, region_dict={}, db=None):
    """
    Fast and correct GeoPolRisk calculation using filtered mapped_baci and pre-cached lookups.
    """
    if db is None:
        raise ValueError("Database instance is required!")

    ecoinvent_mapping = load_ei_mapping()
    regions(region_dict, db)

    # Pre-cache lookups
    resource_name_map = {r: cvtresource(db=db, resource=r, type="Name") for r in resources}
    resource_hs_map = {r: cvtresource(db=db, resource=r, type="HS") for r in resources}
    country_name_map = {c: cvtcountry(db=db, country=c, type="Name") for c in countries}
    country_iso_map = {c: cvtcountry(db=db, country=c, type="ISO") for c in countries}

    # Load mapped_baci and cast types only once
    df = mapped_baci(db).copy()
    df["period"] = df["period"].astype(str)
    df["reporterCode"] = df["reporterCode"].astype(str)
    df["rawMaterial"] = df["rawMaterial"].astype(str)
    df["qty"] = pd.to_numeric(df["qty"], errors="coerce")
    df["cifvalue"] = pd.to_numeric(df["cifvalue"], errors="coerce")

    # Pre-filter once by relevant periods and rawMaterials
    relevant_resource_names = {resource_name_map[r] for r in resources}
    relevant_periods = {str(p) for p in period}
    df = df[df["period"].isin(relevant_periods) & df["rawMaterial"].isin(relevant_resource_names)]

    # Set MultiIndex for fast slicing
    df.set_index(["period", "reporterCode", "rawMaterial"], inplace=True)

    results = []
    total_iterations = len(period) * len(countries) * len(resources)

    for year, importing_country, resource in tqdm(
            itertools.product(period, countries, resources),
            total=total_iterations,
            desc="Calculating GeoPolRisk:",
            unit="iter"
    ):
        try:
            resource_name = resource_name_map[resource]
            resource_code = resource_hs_map[resource]
            country_name = country_name_map[importing_country]
            country_iso = str(country_iso_map[importing_country])
            key_global = (str(year), slice(None), resource_name)
            key_local = (str(year), country_iso, resource_name)
        except Exception as e:
            logging.debug(f"Skipping mapping error: {e}")
            continue

        # Use .loc for fast indexed lookup
        try:
            global_trade = df.loc[key_global]
            relevant_trade = df.loc[key_local]
        except KeyError:
            continue

        if global_trade.empty or relevant_trade.empty:
            continue

        global_price = global_trade["cifvalue"].sum() / global_trade["qty"].sum() if global_trade[
                                                                                         "qty"].sum() > 0 else 0
        exporters = relevant_trade["partnerDesc"].unique()

        try:
            risk_results = importrisk(resource_code, year, importing_country, exporters, df.reset_index(), global_price,
                                      db)
        except Exception as e:
            logging.debug(f"importrisk failed: {e}")
            continue

        try:
            prodqty, hhi = HHI(resource=resource_name, year=year, country=country_name, db=db)
        except Exception as e:
            logging.debug(f"HHI error: {e}")
            continue

        if not risk_results:
            continue

        country_price = risk_results[0].get("CountryPrice", global_price)
        denom = relevant_trade["qty"].sum() + prodqty
        if denom <= 0:
            continue

        global_numerator = 0
        for r in risk_results:
            exporter = r["Exporter"]
            numerator = r["Numerator"]
            global_numerator += numerator

            Score, CF, CF_norm, IR = GeoPolRisk(numerator, denom, country_price, hhi, db=db)
            results.append({
                "Year": year,
                "Importing Country": country_name,
                "Exporting Country": cvtcountry(db=db, country=exporter, type="Name"),
                "Resource HS": resource_code,
                "Resource Name": resource_name,
                "GeoPolRisk Score [-]": Score,
                "GeoPolRisk Characterization Factor [USD/Kg]": CF,
                "GeoPolRisk Characterization Factor Normalized to copper [-]": CF_norm,
                "HHI": hhi,
                "Import Risk": IR,
                "Global Price": global_price,
                "Country Price": country_price,
                "Dataset name": ecoinvent_mapping.get(int(resource_code), {}).get("dataset_name", "Unknown"),
                "Dataset reference product": ecoinvent_mapping.get(int(resource_code), {}).get(
                    "dataset_reference_product", "Unknown"),
                "operator": ecoinvent_mapping.get(int(resource_code), {}).get("operator", "Unknown"),
                "DBID": create_id(resource_code, country_iso, year),
            })

        if global_numerator > 0:
            Score_g, CF_g, CF_norm, IR_g = GeoPolRisk(global_numerator, denom, country_price, hhi, db=db)
            row = results[-1].copy()
            row.update({
                "Exporting Country": "Global",
                "GeoPolRisk Score [-]": Score_g,
                "GeoPolRisk Characterization Factor [USD/Kg]": CF_g,
                "Import Risk": IR_g
            })
            results.append(row)

    df_out = pd.DataFrame(results)
    output_path = Path(db.output_directory) / "results.xlsx"
    try:
        df_out.to_excel(output_path, index=False)
        print(f"Results saved to {output_path}")
    except Exception as e:
        print(f"Error saving Excel: {e}")

