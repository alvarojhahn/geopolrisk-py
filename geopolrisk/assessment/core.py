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
            country_name = cvtcountry(db=db, country=country, type="Name")
            if country_name in proddf["Country"].tolist():
                country_prod = proddf.loc[proddf["Country"] == country_name, str(year)].fillna(0).iloc[0]
            else:
                country_prod = 0  # Default to 0 if no data is available
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

def importrisk(resource: int, year: int, importing_country: str, exporting_country: list, trade_data, global_price, db):
    """
    Calculates the import risk for exporting-importing country pairs and aggregated values.
    """
    results = []

    def replace_func(x):
        """Ensure qty values are numeric and replace invalid entries."""
        try:
            return float(x) if x is not None and x != "NA" else 0.0
        except (ValueError, TypeError):
            return 0.0

    def wgi_func(x):
        """Assign a default WGI value for missing data."""
        if isinstance(x, float):
            return x
        return 0.5 if x is None or isinstance(x, type(None)) or x.strip() == "NA" else x

    try:
        importer_iso = cvtcountry(db=db, country=importing_country, type="ISO")

        country_trade_data = trade_data[
            (trade_data["period"] == year) &
            (trade_data["reporterCode"] == importer_iso) &
            (trade_data["cmdCode"] == str(resource))
        ]

        if not country_trade_data.empty:
            country_trade_data.loc[:, "qty"] = country_trade_data["qty"].apply(replace_func).astype(float)
            country_trade_data.loc[:, "cifvalue"] = country_trade_data["cifvalue"].apply(replace_func).astype(
                float)

            total_qty = country_trade_data["qty"].sum()
            total_val = country_trade_data["cifvalue"].sum()
            country_price = total_val / total_qty if total_qty > 0 else 0.0

        else:
            country_price = 0.0
            logging.debug(
                f"No trade data found for Year={year}, Importing Country={importing_country}. Country Price set to 0.")

        for exporter in exporting_country:
            try:
                exporter_iso = cvtcountry(db=db, country=exporter, type="ISO")

                tradedf = trade_data[
                    (trade_data["period"] == year) &
                    (trade_data["reporterCode"] == importer_iso) &
                    (trade_data["partnerCode"] == exporter_iso) &
                    (trade_data["cmdCode"] == str(resource))
                ]

                if tradedf.empty:
                    logging.debug(
                        f"No trade data for Exporter={exporter}, Year={year}, Resource={resource}. Skipping...")
                    continue

                tradedf.loc[:, "qty"] = tradedf["qty"].apply(replace_func).astype(float)
                tradedf.loc[:, "cifvalue"] = tradedf["cifvalue"].apply(replace_func).astype(float)
                tradedf.loc[:, "partnerWGI"] = tradedf["partnerWGI"].apply(wgi_func).astype(float)

                QTY = tradedf["qty"].tolist()
                WGI = tradedf["partnerWGI"].tolist()

                trade = sum(QTY)
                numerator = sumproduct(QTY, WGI)

                results.append({
                    "Exporter": exporter,
                    "Numerator": numerator,
                    "TotalTrade": trade,
                    "GlobalPrice": global_price,
                    "CountryPrice": country_price,
                })

            except Exception as e:
                logging.debug(
                    f"Error while calculating import risk: {e}, Resource: {resource}, Importing Country: {importing_country}, Year: {year}")

    except Exception as e:
        logging.debug(f"Error while calculating import risk: {e}, Resource: {resource}, Importing Country: {importing_country}, Year: {year}")

    return results


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
    Calculates the GeoPolRisk Score and Characterization Factor.
    """
    try:
        if denominator <= 0:
            return 0, 0, 0  # Default values

        WTA = numerator / denominator
        hhi = hhi if hhi is not None else 0  # Fallback for missing HHI
        Score = hhi * WTA
        CF = Score * price if price > 0 else 0

    except Exception as e:
        logging.debug(f"Error in GeoPolRisk. Inputs: {locals()}, Error: {e}")
        return 0, 0, 0

    return Score, CF, WTA

