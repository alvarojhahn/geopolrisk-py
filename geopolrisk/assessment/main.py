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


def gprs_calc(period: list, countries: list, resources: list, region_dict={}, db=None):
    """
    A single aggregate function performs all calculations and exports the results as an Excel file.
    """

    if db is None:
        raise ValueError("Database instance is required!")

    ecoinvent_mapping = load_ei_mapping()

    preprocessed_trade_data = preprocess_trade_data(period, resources, db)
    regions(region_dict, db)

    results = []
    total_iterations = len(list(itertools.product(period, countries, resources)))
    for year, importing_country, resource in tqdm(itertools.product(period, countries, resources), desc="Calculating the GeoPolRisk: ", unit="iterations", total=total_iterations):

        resource_hs = cvtresource(db=db, resource=resource, type="HS")

        # Fetching ecoinvent mapping
        mapping = ecoinvent_mapping.get(resource, {})
        dataset_name = mapping.get("dataset_name", "Unknown")
        dataset_reference_product = mapping.get("dataset_reference_product", "Unknown")
        operator = mapping.get("operator", "Unknown")

        # Filter global and relevant trade data
        global_trade = preprocessed_trade_data[
            (preprocessed_trade_data["period"] == year) &
            (preprocessed_trade_data["cmdCode"] == str(resource_hs))
        ]
        relevant_trade_data = preprocessed_trade_data[
            (preprocessed_trade_data["period"] == year) &
            (preprocessed_trade_data["reporterCode"] == importing_country) &
            (preprocessed_trade_data["cmdCode"] == str(resource))
        ]

        if global_trade.empty:
            logging.debug(f"No global trade data for Year={year}, Resource={resource_hs}. Skipping...")
            continue
        if relevant_trade_data.empty:
            # logging.debug(f"No relevant trade data for Year={year}, Importer={importing_country}. Skipping...")
            continue

        global_price = (
            global_trade["cifvalue"].sum() / global_trade["qty"].sum()
            if global_trade["qty"].sum() > 0 else 0
        )

        exporters = relevant_trade_data["partnerDesc"].unique()
        risk_results = importrisk(resource, year, importing_country, exporters, preprocessed_trade_data, global_price, db)
        if not risk_results:
            logging.debug(f"No risk results for Year={year}, Importer={importing_country}, Resource={resource}. Skipping...")
            continue

        # Extract country price from the first result of `importrisk`
        country_price = risk_results[0].get("CountryPrice", global_price)

        prodqty, hhi = cached_HHI(resource=resource, year=year, db=db, country= importing_country)

        denominator = relevant_trade_data["qty"].sum() + prodqty
        if denominator <= 0:
            logging.debug(f"Invalid denominator for Year={year}, Importer={importing_country}. Skipping...")
            continue

        global_numerator = 0
        for risk in risk_results:
            exporter = risk["Exporter"]
            numerator = risk["Numerator"]

            # Accumulate for global metrics
            global_numerator += numerator

            Score, CF, CF_norm, IR = GeoPolRisk(numerator, denominator, country_price, hhi, db=db)
            results.append({
                "Year": year,
                "Importing Country": cvtcountry(db=db, country=importing_country, type="Name"),
                "Exporting Country": cvtcountry(db=db, country=exporter, type="Name"),
                "Resource HS": resource,
                "Resource Name": cvtresource(db=db, resource=resource, type="Name"),
                "GeoPolRisk Score [-]": Score,
                "GeoPolRisk Characterization Factor [USD/Kg]": CF,
                "GeoPolRisk Characterization Factor Normalized to copper [-]": CF_norm,
                "HHI": hhi,
                "Import Risk": IR,
                "Global Price": global_price,
                "Country Price": country_price,
                "Dataset name": dataset_name,
                "Dataset reference product": dataset_reference_product,
                "operator": operator,
            })

        # Add the "Global" row
        if global_numerator  > 0:
            Score_global, CF_global, CF_norm, IR_global = GeoPolRisk(global_numerator, denominator, country_price, hhi, db=db)
            results.append({
                "Year": year,
                "Importing Country": cvtcountry(db=db, country=importing_country, type="Name"),
                "Exporting Country": "Global",
                "Resource HS": resource,
                "Resource Name": cvtresource(db=db, resource=resource, type="Name"),
                "GeoPolRisk Score [-]": Score_global,
                "GeoPolRisk Characterization Factor [USD/Kg]": CF_global,
                "GeoPolRisk Characterization Factor Normalized to copper [-]": CF_norm,
                "HHI": hhi,
                "Import Risk": IR_global,
                "Global Price": global_price,
                "Country Price": country_price,
                "Dataset name": dataset_name,
                "Dataset reference product": dataset_reference_product,
                "operator": operator,
            })

    # Save results to Excel
    results_df = pd.DataFrame(results)
    output_path = str(Path(db.output_directory) / "results.xlsx")
    try:
        results_df.to_excel(output_path, index=False)
        print(f"Results successfully saved to {output_path}")
    except Exception as e:
        print(f"Error saving results to Excel: {e}")
