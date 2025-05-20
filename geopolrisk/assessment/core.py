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


from typing import Union
from .database import logging
from .utils import *
from functools import lru_cache
import pandas as pd


def HHI(resource: Union[str, int], year: int, db, country=None):
    """
    Calculates the Herfindahl-Hirschman Index of production of resources,
    normalized to a scale of 0 - 1.
    """
    try:
        # Fetch production data for the resource
        proddf = getProd(resource=resource, db=db)
        if proddf.empty or str(year) not in proddf.columns:
            logging.debug(f"No production data found for resource: {resource}, year: {year}")
            return 0, 0  # Default values

        # Extract production quantities for the given year
        proddf = proddf[proddf["Country_Code"] != "DELETE"]
        prod_year = proddf[str(year)].fillna(0).tolist()

        # Calculate HHI
        HHI_Num = sumproduct(prod_year, prod_year)
        hhi = HHI_Num / (sum(prod_year) ** 2) if sum(prod_year) > 0 else 0

        # Calculate total production quantity
        ProdQty = sum(prod_year)

        # If a country is specified, fetch its production quantity
        if country is not None:
            country_iso = str(cvtcountry(db=db, country=country, type="ISO"))

            country_production = proddf[proddf["Country_Code"].astype(str) == country_iso]

            if not country_production.empty:
                country_prod = country_production[str(year)].fillna(0)
                country_prod = country_prod.iloc[0] if not country_prod.empty else 0
            else:
                logging.debug(f"No production data found for {country}: {country_iso}, year: {year}")
                country_prod = 0  # Default to 0 if no data is available

            # if country_name in proddf["Country_Code"].astype(int).tolist():
            #     country_prod = proddf.loc[proddf["Country"] == country_name, str(year)].fillna(0).iloc[0]
            # else:
            #     country_prod = 0  # Default to 0 if no data is available
        else:
            # If no country is specified, calculate total global production
            country_prod = sum(prod_year)

        # Normalize production quantity based on unit
        if not proddf.empty and "unit" in proddf.columns:
            unit = proddf["unit"].iloc[0]
            if unit == "kg":
                ProdQty /= 1000
            elif unit == "Mio m3":
                ProdQty *= 0.0008
            elif unit not in ["metr. t", "kg"]:
                raise ValueError(f"Unexpected unit: {unit}")

    except Exception as e:
        logging.debug(f"Error while calculating HHI. Resource: {resource}, Year: {year}, Error: {e}")
        return 0, 0  # Default values

    return country_prod, hhi

@lru_cache(maxsize=None)
def cached_HHI(resource, year, db, country):
    try:
        return HHI(resource=resource, year=year, db=db, country=country)
    except Exception as e:
        logging.debug(f"HHI calculation failed: {e}")
        return 0, 0


def importrisk(resource_name: str,
               year: int,
               importing_country: str,
               db,
               hs_map=None,
               baci=None,):
    """
    Returns per-exporter Numerator, TotalTrade, and shared CountryPrice.
    """

    def fix_wgi(x):
        try:
            if x is None or str(x).strip() == "NA":
                return 0.5
            return float(x)
        except:
            return 0.5

    results = []
    if hs_map is None:
        hs_map = Mapping(db)
    if baci is None:
        baci = mapped_baci(db)
    else:
        baci = baci.copy()

    hs_codes = hs_map.get(resource_name, [])
    if not hs_codes:
        logging.debug(f"No HS codes mapped for resource: {resource_name}")
        return [], 0.0, 0.0

    # importer_iso = int(cvtcountry(db=db, country=importing_country, type="ISO"))

    if not hasattr(db, "baci_trade") or db.baci_trade is None:
        db.baci_trade = db.load_databases()["baci"]["baci_trade"]

    if baci.empty:
        logging.debug(f"No BACI data for: {resource_name}, {year}, {importing_country}")
        return [], 0.0, 0.0

    baci["qty"] = pd.to_numeric(baci["qty"].apply(replace_func), errors="coerce")
    baci["cifvalue"] = pd.to_numeric(baci["cifvalue"].apply(replace_func), errors="coerce")
    baci["partnerWGI"] = baci["partnerWGI"].apply(fix_wgi)

    total_qty = baci["qty"].sum()
    total_val = baci["cifvalue"].sum()
    country_price = total_val / total_qty if total_qty > 0 else 0.0

    baci = baci[(baci["qty"] > 0) & (baci["partnerWGI"].notnull())].copy()
    for exp_iso, grp in baci.groupby("partnerCode"):
        exporter = exp_iso
        numerator = (grp["qty"] * grp["partnerWGI"]).sum()
        total_trade = grp["qty"].sum()
        results.append({
            "Exporter": exporter,
            "Numerator": numerator,
            "TotalTrade": total_trade,
        })


    return results, total_qty, country_price


def importrisk_company(resource: int, year: int):
    """
    The 'import risk' for a company differs from that of the country's.
    This data is provided in a template in the output folder.
    The utility function transforms the data into a 
    usable format similar to that of the country-level data.
    """
    tradedf = transformdata()
    df_query = f"(period == {year})  & (cmdCode == {resource})"
    data = tradedf.query(df_query)
    QTY = data["qty"].tolist()
    WGI = data["partnerWGI"].tolist()
    VAL = data["cifvalue"].tolist()
    try:
        Price = sum(VAL) / sum(QTY)
        TotalTrade = sum(QTY)
        Numerator = sumproduct(QTY, WGI)
    except:
        logging.debug(f"Error while making calculations. Resource: {resource}, Country: Company, Year: {year}")
        raise ValueError
    """
    'Numerator' : float
    'TotalTrade' : float
    'Price' : float
    """
    return Numerator, TotalTrade, Price


def GeoPolRisk(numerator, denominator, price, hhi, db):
    """
    Calculates GeoPolRisk Score, CF, normalized CF, and Import Risk (WTA).
    """
    try:
        if denominator <= 0:
            return 0, 0, 0, 0

        CF_Cu = 0.409412948  # Reference CF for copper [USD/kg]

        WTA = numerator / denominator
        Score = hhi * WTA
        CF = Score * price
        CF_norm = CF / CF_Cu if CF > 0 else 0

        return Score, CF, CF_norm, WTA

    except Exception as e:
        logging.debug(f"Error in GeoPolRisk: {e}, Inputs: {locals()}")
        return 0, 0, 0, 0


