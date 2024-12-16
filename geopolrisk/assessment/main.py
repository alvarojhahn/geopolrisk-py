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
from geopolrisk.assessment.database import Database
from geopolrisk.assessment.utils import regions
from .core import *
from .utils import *


def gprs_calc(period: list, country: list, resource: list, region_dict={}, db=None):
    """
    A single aggregate function performs all calculations and exports the results as an Excel file.
    The inputs include a list of years, a list of countries, 
    and a list of resources, with an optional dictionary for defining new regions.
    The lists can contain resource names such as 'Cobalt' and 'Lithium',
    and country names like 'Japan' and 'Canada', or alternatively, HS codes and ISO digit codes.

    For regional assessments, regions must be defined in the dictionary with country names,
    not ISO digit codes.
    For example, the 'West Europe' region can be defined as
    {
        'West Europe': ['France', 'Germany', 'Italy', 'Spain', 'Portugal', 'Belgium', 'Netherlands', 'Luxembourg']
        }.
    """
    if db is None:
        raise ValueError("Database instance is required!")

    preprocessed_trade_data = preprocess_trade_data(period, resource, db)
    preprocessed_production_data = preprocess_production_data(resource, period, db)

    regions(region_dict, db)
    # precomputed_regions = {
    #     cty: db.regionslist[cvtcountry(db=db, country=cty, type="Name")] for cty in country
    # }
    precomputed_regions = {
        cty: db.regionslist.get(cty, [cty])  # Ensure fallback to a single country list
        for cty in country
    }

    country_name_map = {ctry: cvtcountry(db=db, country=ctry, type="Name") for ctry in country}
    country_iso_map = {ctry: cvtcountry(db=db, country=ctry, type="ISO") for ctry in country}
    resource_hs_map = {rm: cvtresource(db=db, resource=rm, type="HS") for rm in resource}
    resource_name_map = {rm: cvtresource(db=db, resource=rm, type="Name") for rm in resource}

    Score_list, CF_list, hhi_list, ir_list, price_list = [], [], [], [], []
    ctry_db, rm_db, period_db, dbid = [], [], [], []

    combinations = list(itertools.product(resource, period, country))
    for idx, (rm, year, ctry) in enumerate(tqdm(combinations, desc="Calculating the GeoPolRisk: ", unit=" iterations")):
        try:
            region = precomputed_regions[ctry]
            ctry_name = country_name_map[ctry]
            ctry_iso = country_iso_map[ctry]
            rm_hs = resource_hs_map[rm]
            rm_name = resource_name_map[rm]

            if len(region) > 1:
                ProdQty, hhi = cached_HHI(rm, year, tuple(region), db=db)
                Numerator, TotalTrade, Price = aggregateTrade(filtered_data=preprocessed_trade_data, year=year, countries=region, commoditycode=rm_hs, db=db)
                Score, CF, IR = GeoPolRisk(Numerator, TotalTrade, Price, ProdQty, hhi, db=db)

            else:
                ProdQty, hhi = cached_HHI(rm, year, ctry, db=db)
                Numerator, TotalTrade, Price = aggregateTrade(filtered_data=preprocessed_trade_data, year=year, countries=[ctry], commoditycode=rm_hs, db=db)
                Score, CF, IR = GeoPolRisk(Numerator, TotalTrade, Price, ProdQty, hhi, db=db)

            try:
                Score_list.append(Score)
                CF_list.append(CF)
                hhi_list.append(hhi)
                ir_list.append(IR)
                price_list.append(Price)
                ctry_db.append(ctry_name)
                rm_db.append(rm_name)
                period_db.append(year)
                dbid.append(create_id(rm_hs, ctry_iso, year))
            except Exception as e:
                logging.debug(f"Error processing Year={year}, Country={ctry}, Resource={rm}: {e}")
                # Append placeholder values to maintain consistent list lengths
                Score_list.append(0)
                CF_list.append(0)
                hhi_list.append(0)
                ir_list.append(0)
                price_list.append(0)
                ctry_db.append(ctry_name)
                rm_db.append(rm_name)
                period_db.append(year)
                dbid.append(create_id(rm_hs, ctry_iso, year))

        except Exception as e:
            logging.debug(f"Error processing Year={year}, Country={ctry}, Resource={rm}: {e}")

    # Create result DataFrame
    result = createresultsdf(db)
    result["DBID"] = dbid
    result["Country [Economic Entity]"] = ctry_db
    result["Raw Material"] = rm_db
    result["Year"] = period_db
    result["GeoPolRisk Score"] = Score_list
    result["GeoPolRisk Characterization Factor [eq. Kg-Cu/Kg]"] = CF_list
    result["HHI"] = hhi_list
    result["Import Risk"] = ir_list
    result["Price"] = price_list

    # Write results to Excel and database
    excel_path = str(Path(db.output_directory) / "results.xlsx")
    result.to_excel(excel_path, index=False)
    try:
        writetodb(db, result)
    except Exception as e:
        logging.debug("Error writing results to database!", e)
