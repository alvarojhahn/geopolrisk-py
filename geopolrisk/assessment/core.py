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


def HHI(resource: Union[str, int], year: int, country: Union[str, tuple], db):
    """
    Calculates the Herfindahl-Hirschman Index of production of resources,
    normalized to a scale of 0 - 1.
    """
    try:
        proddf = getProd(resource=resource, db=db)
        if proddf.empty or str(year) not in proddf.columns:
            logging.debug(f"No production data found for resource: {resource}, year: {year}")
            return 0, 0  # Default values

        proddf = proddf[proddf["Country_Code"] != "DELETE"]
        prod_year = proddf[str(year)].fillna(0).tolist()

        HHI_Num = sumproduct(prod_year, prod_year)
        hhi = HHI_Num / (sum(prod_year) ** 2) if sum(prod_year) > 0 else 0

        if isinstance(country, tuple):  # Region case
            ProdQty = sum(
                proddf.loc[
                    proddf["Country"].isin(
                        [cvtcountry(db=db, country=country, type="Name") for country in country]),
                    str(year)
                ].fillna(0).tolist()
            )
        else:  # Single country case
            ProdQty = proddf.loc[
                proddf["Country"] == cvtcountry(db=db, country=country, type="Name"),
                str(year)
            ].fillna(0).iloc[0] if cvtcountry(db=db, country=country, type="Name") in proddf[
                "Country"].tolist() else 0

        if proddf["unit"].tolist()[0] == "kg":
            ProdQty /= 1000
        elif proddf["unit"].tolist()[0] == "Mio m3":
            ProdQty *= 0.0008
        elif proddf["unit"].tolist()[0] not in ["metr. t", "kg"]:
            raise ValueError(f"Unexpected unit: {proddf['unit'].tolist()[0]}")


    except Exception as e:
        logging.debug(f"Error while calculating HHI. Resource: {resource}, Year: {year}, Country: {country}, Error: {e}")
        return 0, 0  # Default values

    return ProdQty, hhi

@lru_cache(maxsize=None)
def cached_HHI(resource, year, country, db):
    try:
        return HHI(resource, year, country, db)
    except Exception as e:
        logging.debug(f"HHI calculation failed: {e}")
        return 0, 0



def importrisk(resource: int, year: int, country: list, db):
    """
    Calculates the import risk, handling cases with missing data gracefully.
    """
    def wgi_func(x):
        """Assign a default WGI value for missing data."""
        if isinstance(x, float):
            return x
        return 0.5 if x is None or isinstance(x, type(None)) or x.strip() == "NA" else x

    # Initialize variables with default values
    Numerator, TotalTrade, Price = 0, 0, 0

    if not db.regional:
        try:
            ctry = cvtcountry(db=db, country=country[0], type="ISO")
            tradedf = getbacidata(year, ctry, resource, db=db)

            if tradedf.empty:
                logging.debug(f"No trade data found for resource: {resource}, country: {country}, year: {year}")
                return Numerator, TotalTrade, Price  # Skip processing

            QTY = tradedf["qty"].astype(float).tolist()
            WGI = tradedf["partnerWGI"].apply(wgi_func).astype(float).tolist()
            VAL = tradedf["cifvalue"].astype(float).tolist()

            Price = sum(VAL) / sum(QTY) if sum(QTY) > 0 else 0
            TotalTrade = sum(QTY)
            Numerator = sumproduct(QTY, WGI)

        except Exception as e:
            logging.debug(f"Error while calculating import risk: {e}, Resource: {resource}, Country: {country}, Year: {year}")
    else:
        try:
            Numerator, TotalTrade, Price = aggregateTrade(year, country, resource, db=db)
        except Exception as e:
            logging.debug(f"The inputs for calculating the 'import risk' don't match, Country: {country}, Error: {e}")

    return Numerator, TotalTrade, Price


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


def GeoPolRisk(Numerator, TotalTrade, Price, ProdQty, hhi, db):
    """
    Calculates the GeoPolRisk Score and Characterization Factor.
    """
    try:
        Denominator = TotalTrade + ProdQty
        if Denominator <= 0:
            logging.debug(f"Cannot calculate WTA. TotalTrade: {TotalTrade}, ProdQty: {ProdQty}")
            return 0, 0, 0  # Default values

        WTA = Numerator / Denominator
        hhi = hhi if hhi is not None else 0  # Fallback for missing HHI
        Score = hhi * WTA
        CF = Score * Price if Price > 0 else 0 ###### WHERE DO WE NORMALIZE TO COPPER???? UNLESS PRICE IS NORMALIZED TO COPPER

    except Exception as e:
        logging.debug(f"Error in GeoPolRisk. Inputs: {locals()}, Error: {e}")
        return 0, 0, 0

    return Score, CF, WTA
