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
from .database import logging
from .core import *
from .utils import *
from pathlib import Path
import yaml
import pandas as pd
from geopolrisk.assessment.utils import regions

def load_ei_mapping():
    script_dir = Path(__file__).resolve().parent
    yaml_path = script_dir / "ecoinvent_mapping.yaml"
    with open(yaml_path, "r") as file:
        return yaml.safe_load(file)["ecoinvent_mappings"]

def gprs_calc(period: list,
              countries: list,
              rawmaterial: list,
              region_dict={},
              bilateral=False,
              db=None):
    """
    A single aggregate function performs all calculations and exports the results as an Excel file.
    The inputs include a list of years, a list of countries,
    and a list of raw materials, with an optional dictionary for defining new regions.
    The lists should contain rawmaterial names such as 'Cobalt' and 'Lithium',
    and can for country, names like 'Japan' and 'Canada', or ISO digit codes.

    For regional assessments, regions must be defined in the dictionary with country names,
    not ISO digit codes.
    For example, the 'West Europe' region can be defined as
    {
        'West Europe': ['France', 'Germany', 'Italy', 'Spain', 'Portugal', 'Belgium', 'Netherlands', 'Luxembourg']
        }.
    """
    if db is None:
        raise ValueError("Database instance is required!")

    hs_map = Mapping(db)
    ecoinvent_mapping = load_ei_mapping()
    regions(region_dict, db)

    # resource_name_map = {r: r for r in rawmaterial}
    # resource_hs_map = {r: ";".join(map(str, hs_map.get(r, []))) for r in rawmaterial}
    # country_name_map = {c: cvtcountry(db=db, country=c, type="Name") for c in countries}
    # country_iso_map = {c: str(cvtcountry(db=db, country=c, type="ISO")) for c in countries}

    # raw_baci = mapped_baci(db)
    # raw_baci["cmdCode"] = pd.to_numeric(raw_baci["cmdCode"], errors="coerce")
    # df = raw_baci.copy()
    # df["period"] = df["period"].astype(str)
    # df["reporterCode"] = df["reporterCode"].astype(str)
    # df["rawMaterial"] = df["rawMaterial"].astype(str)
    # df["qty"] = pd.to_numeric(df["qty"], errors="coerce")
    # df["cifvalue"] = pd.to_numeric(df["cifvalue"], errors="coerce")

    ######
    raw_baci = mapped_baci(db)
    raw_baci["cmdCode"] = pd.to_numeric(raw_baci["cmdCode"], errors="coerce")
    raw_baci["period"] = raw_baci["period"].astype(str)
    raw_baci["reporterCode"] = raw_baci["reporterCode"].astype(str)
    raw_baci["rawMaterial"] = raw_baci["rawMaterial"].astype(str)
    raw_baci["qty"] = pd.to_numeric(raw_baci["qty"], errors="coerce")
    raw_baci["cifvalue"] = pd.to_numeric(raw_baci["cifvalue"], errors="coerce")

    global_production_map = (
        raw_baci
        .groupby(["period", "rawMaterial"])["qty"]
        .sum()
        .to_dict()
    )
    global_price_map = (
        raw_baci
        .groupby(["period", "rawMaterial"])
        .apply(lambda g: g["cifvalue"].sum() / g["qty"].sum() if g["qty"].sum() > 0 else 0)
        .to_dict()
    )

    all_countries = set(countries).union(raw_baci["reporterCode"].unique(), raw_baci["partnerCode"].unique())
    country_name_map = {}
    country_iso_map = {}
    for c in all_countries:
        try:
            if str(c).isdigit():  # Check if the code is numeric
                country_code = int(c)  # Convert to int for cvtcountry
            else:
                country_code = c  # Pass non-numeric (e.g., names) directly
            country_name_map[c] = cvtcountry(db=db, country=country_code, type="Name")
            country_iso_map[c] = str(cvtcountry(db=db, country=country_code, type="ISO"))
        except Exception as e:
            logging.debug(f"Skipping country code '{c}': {e}")
            continue
    ######


    relevant_periods = {str(p) for p in period}
    # relevant_materials = {resource_name_map[r] for r in rawmaterial}
    relevant_materials = {r for r in rawmaterial}
    # df = df[df["period"].isin(relevant_periods) & df["rawMaterial"].isin(relevant_materials)].copy()

    ###
    relevant_reporters = {country_iso_map[c] for c in countries if c in country_iso_map}
    df = raw_baci[
        (raw_baci["period"].isin(relevant_periods)) &
        (raw_baci["rawMaterial"].isin(relevant_materials)) &
        (raw_baci["reporterCode"].isin(relevant_reporters))
        ].copy()
    ###

    df.set_index(["period", "reporterCode", "rawMaterial"], inplace=True)
    df.sort_index(inplace=True)

    results = []
    total_iterations = len(period) * len(countries) * len(rawmaterial)

    for year, importer, resource in tqdm(
            itertools.product(period, countries, rawmaterial),
            total=total_iterations,
            desc="Calculating GeoPolRisk:",
            unit="iter"
    ):
        try:
            year_str = str(year)
            # resource_name = resource_name_map[resource]
            resource_name = resource
            # resource_hs = resource_hs_map[resource]
            resource_hs = ";".join(map(str, hs_map.get(resource, [])))
            importer_name = country_name_map[importer]
            importer_iso = country_iso_map[importer]
            if db.regional and importer in db.regionslist:
                importer_isos = [str(cvtcountry(db=db, country=c, type="ISO")) for c in db.regionslist[importer]]
                key_local = [(year_str, iso, resource_name) for iso in importer_isos]
            else:
                key_local = [(year_str, importer_iso, resource_name)]
            key_global = (year_str, slice(None), resource_name)
        except Exception as e:
            logging.debug(f"Mapping error, skipping iteration: {e}")
            continue

        try:
            if db.regional and importer in db.regionslist:
                local_trades = [
                    df.loc[(year_str, str(cvtcountry(db=db, country=c, type="ISO")), resource_name)]
                    for c in db.regionslist[importer]
                    if (year_str, str(cvtcountry(db=db, country=c, type="ISO")), resource_name) in df.index
                ]

                if not local_trades:
                    continue

                local_trade = pd.concat(local_trades)
            else:
                local_trade = df.loc[key_local]
            # global_trade = df.loc[key_global]
            global_prod = global_production_map.get((year_str, resource_name), 0)
        except KeyError:
            continue

        if local_trade.empty or global_prod <= 0:
            continue

        # global_price = global_trade["cifvalue"].sum() / global_trade["qty"].sum() if global_trade[
        #                                                                                  "qty"].sum() > 0 else 0
        global_price = global_price_map.get((year_str, resource_name), 0)

        if db.regional and importer in db.regionslist:
            region_isos = [str(cvtcountry(db=db, country=ctry, type="ISO")) for ctry in db.regionslist[importer]]
            reporter_mask = raw_baci["reporterCode"].astype(str).isin(region_isos)

            baci_slice = raw_baci[
                (raw_baci["period"].astype(str) == year_str) &
                reporter_mask &
                (raw_baci["cmdCode"].isin(hs_map.get(resource_name, [])))
                ].copy()

            baci_slice.loc[baci_slice["partnerCode"].astype(str).isin(region_isos), "partnerWGI"] = 0.0
        else:
            reporter_mask = raw_baci["reporterCode"].astype(str) == importer_iso

            baci_slice = raw_baci[
                (raw_baci["period"].astype(str) == year_str) &
                reporter_mask &
                (raw_baci["cmdCode"].isin(hs_map.get(resource_name, [])))
                ].copy()

        try:
            risk_results, total_import_qty, country_price = importrisk(
                rawmaterial=resource_name,
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
            prodqty, hhi = cached_HHI(resource_name, year, importer_name, db)
        except Exception as e:
            logging.debug(f"HHI failed: {e}")
            continue

        if not risk_results:
            continue

        denom_global = total_import_qty + prodqty
        if denom_global <= 0:
            continue

        total_score = 0
        total_ir = 0

        # all_exporters = set(raw_baci["partnerCode"].unique())
        # exporter_name_map = {}
        # for code in all_exporters:
        #     try:
        #         if str(code).isdigit():
        #             name = cvtcountry(db=db, country=int(code), type="Name")
        #             exporter_name_map[code] = name
        #     except Exception as e:
        #         logging.debug(f"[Exporter Mapping] Skipping code {code}: {e}")
        #         continue

        for r in risk_results:
            if r["Numerator"] <= 0:
                continue

            exporter = r["Exporter"]
            numerator = r["Numerator"]

            denom = numerator + prodqty if bilateral else denom_global
            if denom <= 0:
                continue

            total_ir += numerator
            Score, CF, CF_norm, IR = GeoPolRisk(numerator, denom, country_price, hhi, db=db)
            total_score += Score

            results.append({
                "Year": year,
                "Importing Country": importer if db.regional and importer in db.regionslist else importer_name,
                # "Exporting Country": exporter_name_map.get(exporter, "Unknown"),
                "Exporting Country": country_name_map.get(exporter, "Unknown"),
                "Resource HS": resource_hs,
                "Resource Name": resource_name,
                "GeoPolRisk Score [-]": Score,
                "GeoPolRisk Characterization Factor [USD/Kg]": CF,
                "GeoPolRisk Characterization Factor Normalized to copper [-]": CF_norm,
                "HHI": hhi,
                "Import Risk": IR,
                "Global Price": global_price,
                "Country Price": country_price,
                "National Production": prodqty,
                # "Global Production": global_trade["qty"].sum(),
                "Global Production": global_prod,
                "Specific Imports": numerator,
                "Total Imports": total_import_qty,
                "Dataset name": ecoinvent_mapping.get(resource_name, {}).get("dataset_name", "Unknown"),
                "Dataset reference product": ecoinvent_mapping.get(resource_name, {}).get("dataset_reference_product", "Unknown"),
                "Operator": ecoinvent_mapping.get(resource_name, {}).get("operator", "Unknown"),
                "Excludes": "; ".join(ecoinvent_mapping.get(resource_name, {}).get("excludes", [])),
                "DBID": create_id(resource, importer_iso, year)
            })

        if total_ir > 0:
            Score_g, CF_g, CF_norm_g, IR_g = GeoPolRisk(total_ir, denom_global, country_price, hhi, db=db)
            row = results[-1].copy()
            row.update({
                "Exporting Country": "Global",
                "GeoPolRisk Score [-]": Score_g,
                "GeoPolRisk Characterization Factor [USD/Kg]": CF_g,
                "GeoPolRisk Characterization Factor Normalized to copper [-]": CF_norm_g,
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
