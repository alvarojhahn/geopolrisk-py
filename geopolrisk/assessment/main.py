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
    Efficient GeoPolRisk calculation with consistent output and pre-filtered data.
    """
    if db is None:
        raise ValueError("Database instance is required!")

        # Load mappings and region setup
    hs_map = Mapping(db)
    ecoinvent_mapping = load_ei_mapping()
    regions(region_dict, db)

    # Pre-cache name/ISO/HS mappings
    resource_name_map = {r: r for r in resources}
    resource_hs_map = {r: ";".join(map(str, hs_map.get(r, []))) for r in resources}
    country_name_map = {c: cvtcountry(db=db, country=c, type="Name") for c in countries}
    country_iso_map = {c: str(cvtcountry(db=db, country=c, type="ISO")) for c in countries}

    # Load and pre-filter trade data
    raw_baci = mapped_baci(db)


    raw_baci["cmdCode"] = pd.to_numeric(raw_baci["cmdCode"], errors="coerce")

    df = raw_baci.copy()
    df["period"] = df["period"].astype(str)
    df["reporterCode"] = df["reporterCode"].astype(str)
    df["rawMaterial"] = df["rawMaterial"].astype(str)
    df["qty"] = pd.to_numeric(df["qty"], errors="coerce")
    df["cifvalue"] = pd.to_numeric(df["cifvalue"], errors="coerce")

    relevant_periods = {str(p) for p in period}
    relevant_materials = {resource_name_map[r] for r in resources}
    df = df[df["period"].isin(relevant_periods) & df["rawMaterial"].isin(relevant_materials)].copy()
    df.set_index(["period", "reporterCode", "rawMaterial"], inplace=True)
    df.sort_index(inplace=True)

    results = []
    total_iterations = len(period) * len(countries) * len(resources)

    for year, importer, resource in tqdm(
            itertools.product(period, countries, resources),
            total=total_iterations,
            desc="Calculating GeoPolRisk:",
            unit="iter"
    ):
        try:
            year_str = str(year)
            resource_name = resource_name_map[resource]
            resource_hs = resource_hs_map[resource]
            importer_name = country_name_map[importer]
            importer_iso = country_iso_map[importer]
            key_local = (year_str, importer_iso, resource_name)
            key_global = (year_str, slice(None), resource_name)
        except Exception as e:
            logging.debug(f"Mapping error, skipping iteration: {e}")
            continue

        try:
            local_trade = df.loc[key_local]
            global_trade = df.loc[key_global]
        except KeyError:
            continue

        if local_trade.empty or global_trade.empty:
            continue

        global_price = global_trade["cifvalue"].sum() / global_trade["qty"].sum() if global_trade[
                                                                                         "qty"].sum() > 0 else 0

        baci_slice = raw_baci[
            (raw_baci["period"].astype(str) == year_str) &
            (raw_baci["reporterCode"].astype(str) == importer_iso) &
            # (raw_baci["rawMaterial"] == resource_name)
            (raw_baci["cmdCode"].isin(hs_map.get(resource_name, [])))
            ]

        try:
            risk_results, total_import_qty, country_price = importrisk(
                resource_name=resource_name,
                year=year,
                importing_country=importer,
                db=db,
                hs_map=hs_map,
                baci=baci_slice,
            )
        except Exception as e:
            logging.debug(f"importrisk failed: {e}")
            continue

        try:
            prodqty, hhi = HHI(resource=resource_name, year=year, country=importer_name, db=db)
        except Exception as e:
            logging.debug(f"HHI failed: {e}")
            continue

        if not risk_results:
            continue

        denom = total_import_qty + prodqty
        if denom <= 0:
            continue

        total_score = 0
        total_ir = 0

        all_exporters = set(raw_baci["partnerCode"].unique())
        exporter_name_map = {
            code: cvtcountry(db=db, country=code, type="Name")
            for code in all_exporters
        }

        for r in risk_results:
            exporter = r["Exporter"]
            numerator = r["Numerator"]
            total_ir += numerator

            Score, CF, CF_norm, IR = GeoPolRisk(numerator, denom, country_price, hhi, db=db)
            total_score += Score

            results.append({
                "Year": year,
                "Importing Country": importer_name,
                "Exporting Country": exporter_name_map.get(exporter, "Unknown"),
                "Resource HS": resource_hs,
                "Resource Name": resource_name,
                "GeoPolRisk Score [-]": Score,
                "GeoPolRisk Characterization Factor [USD/Kg]": CF,
                "GeoPolRisk Characterization Factor Normalized to copper [-]": CF_norm,
                "HHI": hhi,
                "Import Risk": IR,
                "Global Price": global_price,
                "Country Price": country_price,
                "Dataset name": ecoinvent_mapping.get(resource_name, {}).get("dataset_name", "Unknown"),
                "Dataset reference product": ecoinvent_mapping.get(resource_name, {}).get("dataset_reference_product",
                                                                                          "Unknown"),
                "operator": ecoinvent_mapping.get(resource_name, {}).get("operator", "Unknown"),
                "DBID": create_id(resource, importer_iso, year)
            })

        # Add global summary row
        if total_ir > 0:
            Score_g, CF_g, CF_norm_g, IR_g = GeoPolRisk(total_ir, denom, country_price, hhi, db=db)
            row = results[-1].copy()
            row.update({
                "Exporting Country": "Global",
                "GeoPolRisk Score [-]": Score_g,
                "GeoPolRisk Characterization Factor [USD/Kg]": CF_g,
                "GeoPolRisk Characterization Factor Normalized to copper [-]": CF_norm_g,
                "Import Risk": IR_g
            })
            results.append(row)

    # Export results
    df_out = pd.DataFrame(results)
    output_path = Path(db.output_directory) / "results.xlsx"
    try:
        df_out.to_excel(output_path, index=False)
        print(f"Results saved to {output_path}")
    except Exception as e:
        print(f"Error saving Excel: {e}")

    # if db is None:
    #     raise ValueError("Database instance is required!")
    #
    # hs_map = Mapping(db)
    # ecoinvent_mapping = load_ei_mapping()
    # regions(region_dict, db)
    #
    # # Pre-cache lookups
    # resource_name_map = {r: r for r in resources}
    # resource_hs_map = {r: ";".join(map(str, hs_map.get(r, []))) for r in resources}
    # country_name_map = {c: cvtcountry(db=db, country=c, type="Name") for c in countries}
    # country_iso_map = {c: cvtcountry(db=db, country=c, type="ISO") for c in countries}
    #
    # # Load harmonized trade data
    # df = mapped_baci(db).copy()
    # df["period"] = pd.to_numeric(df["period"], errors="coerce").astype("Int64")
    # df["reporterCode"] = pd.to_numeric(df["reporterCode"], errors="coerce").astype("Int64")
    # df["partnerCode"] = pd.to_numeric(df["partnerCode"], errors="coerce").astype("Int64")
    # df["qty"] = pd.to_numeric(df["qty"], errors="coerce")
    # df["cifvalue"] = pd.to_numeric(df["cifvalue"], errors="coerce")
    #
    # # Price maps
    # global_trade = df.groupby(["period", "rawMaterial"])
    # global_price_map = {
    #     (year, material): (g.cifvalue.sum() / g.qty.sum() if g.qty.sum() > 0 else 0)
    #     for (year, material), g in global_trade
    # }
    #
    # local_trade = df.groupby(["period", "reporterCode", "rawMaterial"])
    # local_map = {
    #     (year, rep, material): g
    #     for (year, rep, material), g in local_trade
    # }
    #
    # results = []
    # total_iterations = len(period) * len(countries) * len(resources)
    # for year, importer, resource in tqdm(
    #         itertools.product(period, countries, resources),
    #         total=total_iterations,
    #         desc="Calculating GeoPolRisk:",
    #         unit="iter"
    # ):
    #     try:
    #         resource_name = resource_name_map[resource]
    #         importer_name = country_name_map[importer]
    #         importer_iso = int(country_iso_map[importer])
    #     except Exception as e:
    #         logging.debug(f"Skipping mapping error: {e}")
    #         continue
    #
    #     metadata = ecoinvent_mapping.get(resource_name, {})
    #     dataset_name = metadata.get("dataset_name", "Unknown")
    #     dataset_ref_product = metadata.get("dataset_reference_product", "Unknown")
    #     operator = metadata.get("operator", "Unknown")
    #
    #     key_global = (year, resource_name)
    #     key_local = (year, importer_iso, resource_name)
    #
    #     global_price = global_price_map.get(key_global)
    #     relevant_data = local_map.get(key_local)
    #
    #     if global_price is None or relevant_data is None or relevant_data.empty:
    #         logging.debug(f"No trade data found for Year={year}, Importing Country={importer}, Resource={resource}.")
    #         continue
    #
    #     try:
    #         risk_results, total_import_qty, country_price = importrisk(
    #             resource_name=resource_name,
    #             year=year,
    #             importing_country=importer,
    #             db=db,
    #             hs_map=hs_map)
    #     except Exception as e:
    #         logging.debug(f"importrisk failed: {e}")
    #         continue
    #
    #     try:
    #         prodqty, hhi = HHI(resource=resource_name, year=year, country=importer_name, db=db)
    #     except Exception as e:
    #         logging.debug(f"HHI error: {e}")
    #         continue
    #
    #     if not risk_results:
    #         continue
    #
    #     denom = total_import_qty + prodqty
    #     if denom <= 0:
    #         continue
    #
    #     total_score = 0
    #     total_ir = 0
    #
    #     for r in risk_results:
    #         exporter = r["Exporter"]
    #         numerator = r["Numerator"]
    #         total_ir += numerator
    #
    #         Score, CF, CF_norm, IR = GeoPolRisk(numerator, denom, country_price, hhi, db=db)
    #         total_score += Score
    #
    #         results.append({
    #             "Year": year,
    #             "Importing Country": importer_name,
    #             "Exporting Country": cvtcountry(db=db, country=exporter, type="Name"),
    #             "Resource HS": resource_hs_map[resource],
    #             "Resource Name": resource_name,
    #             "GeoPolRisk Score [-]": Score,
    #             "GeoPolRisk Characterization Factor [USD/Kg]": CF,
    #             "GeoPolRisk Characterization Factor Normalized to copper [-]": CF_norm,
    #             "HHI": hhi,
    #             "Import Risk": IR,
    #             "Global Price": global_price,
    #             "Country Price": country_price,
    #             "Dataset name": dataset_name,
    #             "Dataset reference product": dataset_ref_product,
    #             "operator": operator,
    #             "DBID": create_id(resource, importer_iso, year)
    #         })
    #
    #     # Global row based on disaggregated sum
    #     Score_g, CF_g, CF_norm_g, IR_g = GeoPolRisk(total_ir, denom, country_price, hhi, db=db)
    #
    #     row = results[-1].copy()
    #     row.update({
    #         "Exporting Country": "Global",
    #         "GeoPolRisk Score [-]": Score_g,
    #         "GeoPolRisk Characterization Factor [USD/Kg]": CF_g,
    #         "GeoPolRisk Characterization Factor Normalized to copper [-]": CF_norm_g,
    #         "Import Risk": IR_g
    #     })
    #     results.append(row)
    #
    # df_out = pd.DataFrame(results)
    # output_path = Path(db.output_directory) / "results.xlsx"
    # try:
    #     df_out.to_excel(output_path, index=False)
    #     print(f"Results saved to {output_path}")
    # except Exception as e:
    #     print(f"Error saving Excel: {e}")


