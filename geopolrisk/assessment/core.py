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


def HHI(rawmaterial: str,
        year: int,
        db,
        country=None):
    """
    Calculates the Herfindahl-Hirschman index of production of raw materials.
    which is normalized to the scale of 0 - 1.
    The dataframe is fetched from a utlity function.
    """
    proddf = getProd(rawmaterial=rawmaterial, db=db)
    proddf = proddf[proddf["Country_Code"] != "DELETE"]
    if proddf.empty or str(year) not in proddf.columns:
        return 0, 0
    prod_year = proddf[str(year)].fillna(0).tolist()

    HHI_Num = sumproduct(prod_year, prod_year)

    try:
        hhi = HHI_Num / (sum(prod_year) ** 2) if sum(prod_year) > 0 else 0
    except:
        logging.debug(
            f"Error while calculating the HHI. Raw Material : {rawmaterial}, year : {year}"
        )
        raise ValueError

    ProdQty = 0
    try:
        if db.regional and country in db.regionslist:
            # Sum across all countries in the region
            for subcountry in db.regionslist[country]:
                iso_code = cvtcountry(db=db, country=subcountry, type="ISO")
                if iso_code in proddf["Country_Code"].astype(int).tolist():
                    ProdQty += proddf.loc[
                        proddf["Country_Code"].astype(int) == iso_code, str(year)
                    ].fillna(0).sum()
        else:
            iso_code = cvtcountry(db=db, country=country, type="ISO")
            if iso_code in proddf["Country_Code"].astype(int).tolist():
                ProdQty = float(
                    proddf.loc[
                        proddf["Country_Code"].astype(int) == iso_code,
                        str(year)
                    ].iloc[0]
                )
    except Exception as e:
        logging.debug(f"Error extracting production quantity for {rawmaterial}, {year}, {country}: {e}")
        ProdQty = 0

    if proddf["unit"].tolist()[0] == "kg":
        ProdQty = ProdQty / 1000
    elif proddf["unit"].tolist()[0] != "metr. t" and proddf["unit"].tolist()[0] != "kg":
        logging.info("Raw material not in metric tonne or kilos")
    elif proddf["unit"].tolist()[0] == "Mio m3":
        """
        1 m³ = 0.8 kg = 0.0008 metr. t
        """
        ProdQty = ProdQty * 0.0008

    """
    The output includes the production quantity of a raw material for a country in a given year and the Herfindahl-Hirschman Indexfor that year.
    'ProdQty' : float
    'hhi': float
    """
    return ProdQty, hhi


_hhi_cache = {}
def cached_HHI(rawmaterial, year, country, db):
    key = (rawmaterial, year, country)
    if key in _hhi_cache:
        return _hhi_cache[key]
    try:
        result = HHI(rawmaterial=rawmaterial, year=year, db=db, country=country)
        _hhi_cache[key] = result
        return result
    except Exception as e:
        logging.debug(f"HHI calculation failed: {e}")
        return 0, 0


def importrisk(rawmaterial: str,
               year: int,
               importing_country: str,
               db,
               hs_map=None,
               baci=None):
    """
    The second part of the equation of the GeoPolRisk method is referred to as 'import risk'.
    This involves weighting the import quantity with the political stability score.
    The political stability score is derived from the
    Political Stability and Absence of Violence indicator of the Worldwide Governance Indicators.
    For more information, see Koyamparambath et al. (2024).
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

    hs_codes = hs_map.get(rawmaterial, [])
    if not hs_codes:
        logging.debug(f"No HS codes mapped for resource: {rawmaterial}")
        return [], 0.0, 0.0

    if not hasattr(db, "baci_trade") or db.baci_trade is None:
        db.baci_trade = db.load_databases()["baci"]["baci_trade"]

    if db.regional and importing_country in db.regionslist:
        # Regional logic
        region_isos = [str(cvtcountry(db=db, country=c, type="ISO")) for c in db.regionslist[importing_country]]
        df_import = baci.loc[baci["reporterCode"].astype(str).isin(region_isos)].copy()
    else:
        importer_iso = str(cvtcountry(db=db, country=importing_country, type="ISO"))
        df_import = baci.loc[baci["reporterCode"].astype(str) == importer_iso].copy()

    if df_import.empty:
        logging.debug(f"No BACI data for {rawmaterial}, {year}, {importing_country}")
        return [], 0.0, 0.0

    df_import["qty"] = pd.to_numeric(df_import["qty"].apply(replace_func), errors="coerce")
    df_import["cifvalue"] = pd.to_numeric(df_import["cifvalue"].apply(replace_func), errors="coerce")
    df_import["partnerWGI"] = df_import["partnerWGI"].apply(fix_wgi)

    total_qty = df_import["qty"].sum()
    total_val = df_import["cifvalue"].sum()
    country_price = total_val / total_qty if total_qty > 0 else 0.0

    df_import = df_import[(df_import["qty"] > 0) & (df_import["partnerWGI"].notnull())].copy()

    for exp_iso, grp in df_import.groupby("partnerCode"):
        exporter = exp_iso
        numerator = (grp["qty"] * grp["partnerWGI"]).sum()
        total_trade = grp["qty"].sum()
        results.append({
            "Exporter": exporter,
            "Numerator": numerator,
            "TotalTrade": total_trade,
        })

    return results, total_qty, country_price


def importrisk_company(rawmaterial: str, year: int):
    """
    The 'import risk' for a company differs from that of the country's.
    This data is provided in a template in the output folder.
    The utility function transforms the data into a
    usable format similar to that of the country-level data.
    """
    tradedf = transformdata()
    df_query = f"(period == {year}) & (rawMaterial == '{rawmaterial}')"

    data = tradedf.query(df_query)
    print(data)
    QTY = data["qty"].astype(float).tolist()
    WGI = data["partnerWGI"].astype(float).tolist()
    VAL = data["cifvalue"].astype(float).tolist()
    try:
        Price = sum(VAL) / sum(QTY)
        TotalTrade = sum(QTY)
        Numerator = sumproduct(QTY, WGI)
    except:
        logging.debug(
            f"Error while making calculations. Raw Material: {rawmaterial}, Country: Company, Year: {year}"
        )
        raise ValueError
    """
    'Numerator' : float
    'TotalTrade' : float
    'Price' : float
    """
    return Numerator, TotalTrade, Price


def GeoPolRisk(numerator, denominator, price, hhi, db=None):
    """
    The GeoPolRisk method has two value outputs: the GeoPolRisk Score,
    a non-dimensional score useful for comparative risk assessment,
    and the characterization factor, which is used for evaluating
    the GeoPolitical Supply Risk in Life Cycle Assessment with units of eq. kg-Cu/kg.
    """
    try:
        if denominator <= 0:
            logging.debug(
                f"Denominator is zero or negative. Inputs: {locals()}"
            )
            return 0, 0, 0, 0

        CF_Cu = 0.409412948  # Average CF of copper for OECD countries for a period from 2017 - 2021
        WTA = numerator / denominator
        Score = hhi * WTA
        CF = (Score * price)
        CF_norm = CF / CF_Cu

        return Score, CF, CF_norm, WTA


    except Exception as e:
        logging.debug(f"Error in GeoPolRisk calculation: {e}, Inputs: {locals()}")
        return 0, 0, 0, 0
