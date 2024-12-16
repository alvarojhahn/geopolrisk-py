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

import pandas as pd
import os
from .database import logging, execute_query
from pathlib import Path
from geopolrisk.assessment.database import Database

tradepath = None


# db = str(Path(Database().output_directory))

########################################
##   Utility functions --  GeoPolRisk ##
########################################


def replace_func(x):
    if isinstance(x, float):
        return x
    else:
        if x is None or isinstance(x, type(None)) or x.strip() == "NA" or x == " ":
            return 0
        else:
            return x


def cvtresource(db, resource, type="HS"):
    # Function to convert resource inputs, HS to name or name to HS
    """
    Type can be either 'HS' or 'Name'
    """
    MapHSdf = db.production["HS Code Map"]
    if type == "HS":
        if resource in MapHSdf["ID"].tolist():
            return int(MapHSdf.loc[MapHSdf["ID"] == resource, "HS Code"].iloc[0])
        else:
            try:
                resource = int(resource)
            except Exception as e:
                print(f"To HS: Entered raw material {resource} does not exist in our database!")
                logging.debug(f"To HS: Entered raw material {resource} does not exist in our database!, {e}")
                raise ValueError
            if str(resource) in MapHSdf["HS Code"].tolist():
                return int(resource)
    elif type == "Name":
        try:
            resource = int(resource)
        except:
            # print(f"Entered raw material '{resource}' does not exist in our database! Please enter numerical inputs")
            logging.debug(
                f"Entered raw material '{resource}' does not exist in our database! Please enter numerical inputs")
            resource = str(resource)
        if str(resource) in MapHSdf["HS Code"].astype(str).tolist():
            return MapHSdf.loc[MapHSdf["HS Code"] == str(resource), "ID"].iloc[0]
        elif resource in MapHSdf["ID"].tolist():
            return resource
        else:
            print(f"To Name: Entered raw material '{resource}' does not exist in our database!")
            logging.debug(f"To Name: Entered raw material '{resource}' does not exist in our database!")
            raise ValueError


def cvtcountry(db, country, type="ISO"):
    """
    Type can be either 'ISO' or 'Name'
    """
    MapISOdf = db.production["Country_ISO"]
    try:
        if type == "ISO":
            if country in MapISOdf["Country"].tolist():
                return MapISOdf.loc[MapISOdf["Country"] == country, "ISO"].iloc[0]
            elif db.regional and country in db.regionslist:
                return country
            country = int(country)
            if country in MapISOdf["ISO"].astype(int).tolist():
                return country
            else:
                raise ValueError(f"Country '{country}' cannot be converted to ISO.")

        elif type == "Name":
            try:
                country = int(country)
            except ValueError:
                country = str(country)
            if country in MapISOdf["ISO"].astype(int).tolist():
                return MapISOdf.loc[MapISOdf["ISO"] == country, "Country"].iloc[0]
            elif country in MapISOdf["Country"].tolist():
                return country
            elif db.regional and country in db.regionslist:
                return country
            else:
                raise ValueError(f"Country '{country}' cannot be converted to Name.")
        else:
            raise ValueError(f"Unsupported type '{type}'. Use 'ISO' or 'Name'.")

    except Exception as e:
        logging.debug(f"Error converting country '{country}' to {type}: {e}")
        raise


def sumproduct(A: list, B: list):
    return sum(i * j for i, j in zip(A, B))


def create_id(HS, ISO, Year):
    return str(HS) + str(ISO) + str(Year)


# 2024-08-23 - this function is not being used - deleted
# Verify if the calculation is already stored in the database to avoid recalculation
# def sqlverify(DBID):
#     try:
#         sql = f"SELECT * FROM recordData WHERE id = '{DBID}';"
#         row = execute_query(
#             f"SELECT * FROM recordData WHERE id = '{DBID}';",
#             db_path=db,
#         )
#     except Exception as e:
#         logging.debug(f"Database error in sqlverify - {e}, {sql}")
#         row = None
#     if not row:
#         return False
#     else:
#         return True

def createresultsdf(db):
    dbpath = str(Path(db.output_file))

    # Columns for the dataframe
    Columns = [
        "DBID",
        "Country [Economic Entity]",
        "Raw Material",
        "Year",
        "GeoPolRisk Score",
        "GeoPolRisk Characterization Factor [eq. Kg-Cu/Kg]",
        "HHI",
        "Import Risk",
        "Price",
    ]
    df = pd.DataFrame(columns=Columns)
    SQLQuery = """CREATE TABLE IF NOT EXISTS "recordData" (
            "DBID"	TEXT,
        	"Country [Economic Entity]"	TEXT,
        	"Raw Material"	TEXT,
        	"Year"	INTEGER,
        	"GeoPolRisk Score"	REAL,
        	"GeoPolRisk Characterization Factor [eq. Kg-Cu/Kg]"	REAL,
        	"HHI"	REAL,
        	"Import Risk" REAL,
        	"Price"	INTEGER,
        	PRIMARY KEY("DBID")
        );"""
    row = execute_query(
        SQLQuery,
        db_path=dbpath,
    )
    return df


def writetodb(db, dataframe):
    dbpath = str(Path(db.output_file))
    for index, row in dataframe.iterrows():
        check_query = "SELECT 1 FROM recordData WHERE DBID = ?;"
        exists = execute_query(check_query, db_path=dbpath, params=(row['DBID'],))

        if exists:
            update_query = '''
                UPDATE recordData
                SET 
                    "Country [Economic Entity]" = ?,
                    "Raw Material" = ?,
                    Year = ?,
                    "GeoPolRisk Score" = ?,
                    "GeoPolRisk Characterization Factor [eq. Kg-Cu/Kg]" = ?,
                    HHI = ?,
                    "Import Risk" = ?,
                    Price = ?
                WHERE DBID = ?;
            '''
            params = (
                row['Country [Economic Entity]'],
                row['Raw Material'],
                row['Year'],
                row['GeoPolRisk Score'],
                row['GeoPolRisk Characterization Factor [eq. Kg-Cu/Kg]'],
                row['HHI'],
                row['Import Risk'],
                row['Price'],
                row['DBID']
            )
            try:
                execute_query(update_query, db_path=dbpath, params=params)
            except Exception as e:
                print("Failed to write to output database, Check logs!")
                logging.debug(
                    f"Failed to write to output database - Update Query - Dataframe index = {index} | Error = {e}")
        else:
            insert_query = '''
                INSERT INTO recordData (
                    DBID,
                    "Country [Economic Entity]",
                    "Raw Material",
                    Year,
                    "GeoPolRisk Score",
                    "GeoPolRisk Characterization Factor [eq. Kg-Cu/Kg]",
                    HHI,
                    "Import Risk",
                    Price
                ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?);
            '''
            params = (
                row['DBID'],
                row['Country [Economic Entity]'],
                row['Raw Material'],
                row['Year'],
                row['GeoPolRisk Score'],
                row['GeoPolRisk Characterization Factor [eq. Kg-Cu/Kg]'],
                row['HHI'],
                row['Import Risk'],
                row['Price']
            )
            try:
                execute_query(insert_query, db_path=dbpath, params=params)
            except Exception as e:
                print("Failed to write to output database, Check logs!")
                logging.debug(
                    f"Failed to write to output database - Insert Query - Dataframe index = {index} | Error = {e}")


###################################################
##   Extrade trade data functions --  GeoPolRisk ##
###################################################


def getbacidata(
        period: float, country: float, commoditycode: float, db=None
):
    """
    get the baci-trade-data from the baci_trade dataframe
    """
    if db.baci_trade is None:
        logging.debug("Loading 'baci_trade' dynamically.")
        db.baci_trade = db.load_databases()["baci"]["baci_trade"]

    data = db.baci_trade
    if data is None or data.empty:
        logging.error("The 'baci_trade' table is empty or not loaded.")
        raise RuntimeError("Database is missing required table: baci_trade")

    period = str(period)
    country = str(country)
    commoditycode = str(commoditycode)
    df_query = f"(period == '{period}') & (reporterCode == '{country}') & (cmdCode == '{commoditycode}')"
    baci_data = data.query(df_query)
    """
    The dataframe is structured as follows:
    'period' -> The year of the trade recorded
    'reporterCode' -> The ISO 3 digit code of the reporting country
    'reporterISO' -> The ISO code of the reporting country
    'reporterDesc' -> The name of the reporting country
    'partnerCode' -> The ISO 3 digit code of the partner country
    'partnerISO' -> The ISO code of the partner country
    'partnerDesc' -> The name of the partner country
    'cmdCode' -> The 6 digit commodity code (HS92)
    'qty' -> The trade quantity in 1000 kilograms
    'cifvalue' -> The value of the traded quantity in 1000 USD
    'partnerWGI' -> The WGI political stability and absence of violence indicator (Normalized) for the partner country
    """
    baci_data.loc[:, "qty"] = baci_data["qty"].apply(replace_func).astype(float)
    baci_data.loc[:, "cifvalue"] = baci_data["cifvalue"].apply(replace_func).astype(float)
    if baci_data is None or isinstance(baci_data, type(None)) or len(baci_data) == 0:
        logging.debug(
            f"Problem with get the baci-data - {baci_data} - period == '{period}') - reporterCode == '{country}' - cmdCode == '{commoditycode}'"
        )
        baci_data = None
    return baci_data


def aggregateTrade(filtered_data, year, countries, commoditycode, db=None):
    """
    Aggregate trade data for a region.

    Parameters:
    - filtered_data: Preprocessed trade data (DataFrame).
    - countries: List of countries for the region.
    - commoditycode: HS Code of the commodity (string/int).

    Returns:
    - numerator: Weighted quantity based on WGI.
    - qty: Total trade quantity.
    - price: Average price (value/quantity).
    """

    total_qty, total_value, total_numerator = 0, 0, 0

    for country in countries:
        country_code = cvtcountry(db=db, country=country, type="ISO")
        country_data = filtered_data[
            (filtered_data["period"] == str(year)) &
            (filtered_data["cmdCode"] == str(commoditycode)) &
            (filtered_data["reporterCode"] == str(country_code))
            ]

        if country_data.empty:
            logging.debug(f"No data for country={country}, year={year}")
            continue

        # Aggregate values for this country
        country_data.loc[:, "qty"] = country_data["qty"].apply(replace_func).astype(float)
        country_data.loc[:, "cifvalue"] = country_data["cifvalue"].apply(replace_func).astype(float)
        country_data.loc[:, "partnerWGI"] = country_data["partnerWGI"].apply(replace_func).astype(float)

        qty = country_data["qty"].sum()
        value = country_data["cifvalue"].sum()
        numerator = (country_data["qty"] * country_data["partnerWGI"]).sum()

        total_qty += qty
        total_value += value
        total_numerator += numerator

    # Calculate the price for the entire region
    price = total_value / total_qty if total_qty > 0 else 0

    return total_numerator, total_qty, price


def preprocess_trade_data(periods, resources, db):
    """
    Preprocess trade data for faster lookups.
    Returns a DataFrame filtered by periods and resources.
    """
    if not hasattr(db, "baci_trade") or db.baci_trade is None:
        logging.debug("Loading 'baci_trade' dynamically.")
        db.baci_trade = db.load_databases()["baci"]["baci_trade"]

    if db.baci_trade is None or db.baci_trade.empty:
        logging.error("The 'baci_trade' table is missing or empty.")
        raise RuntimeError("Database is missing required table: baci_trade")

    # Filter relevant rows for requested periods and resources
    resource_codes = [str(cvtresource(db=db, resource=rm, type="HS")) for rm in resources]
    filtered_data = db.baci_trade[
        db.baci_trade["period"].isin(map(str, periods)) &
        db.baci_trade["cmdCode"].isin(resource_codes)
        ]

    if filtered_data.empty:
        logging.warning("Filtered trade data is empty. Verify inputs.")
    else:
        logging.debug(f"Filtered trade data size: {len(filtered_data)} rows.")

    return filtered_data


def preprocess_production_data(resources, periods, db):
    """
    Preprocess production data for all resources and periods.
    """
    logging.debug("Preprocessing production data.")
    production_data = {}
    for rm in resources:
        for year in periods:
            try:
                prod_data = getProd(rm, db)
                if prod_data.empty:
                    logging.warning(f"Production data for resource {rm} is empty.")
                else:
                    for country in prod_data["Country"].unique():
                        country_prod = prod_data.loc[prod_data["Country"] == country, str(year)].sum()
                        production_data[(rm, year, country)] = country_prod
            except Exception as e:
                logging.error(f"Error fetching production data for resource={rm}, year={year}: {e}")
    return production_data


###########################################################
## Converting company trade data into a usable dataframe ##
###########################################################


def transformdata(db, mode="prod"):
    folder_path = str(Path(db.geopolrisk_root / "lib"))
    file_name = "Company data.xlsx"
    excel_sheet_name = "Template"
    # file_path = glob.glob(os.path.join(folder_path, file_name))[0]
    file_path = os.path.join(folder_path, file_name)
    # in test-mode - use the excel-file from the test-folder
    if "test" in mode:
        test_dir = os.path.abspath("./geopolrisk/tests/")
        file_path = f"{test_dir}/{file_name}"
        excel_sheet_name = "Test"
    """
    The template excel file has the following headers
    'Metal': Specify the type of metal.
    'Country of Origin': Indicate the country where the metal was sourced.
    'Quantity (kg)': Enter the quantity of metal imported from each country.
    'Value (USD)': Value of the metal of the quantity imported.
    'Year': The year of the trade
    'Additional Notes': Include any additional relevant information.
    """
    Data = pd.read_excel(file_path, sheet_name=excel_sheet_name)
    HS_Code = []
    for resource in Data["Metal"].tolist():
        HS_Code.append(cvtresource(db=db, resource=resource, type="HS"))
    ISO = []
    for country in Data["Country of Origin"].tolist():
        ISO.append(cvtcountry(db=db, country=country, type="ISO"))
    MapWGIdf = db.wgi
    wgi = []
    for i, iso in enumerate(ISO):
        try:
            wgi.append(
                # MapWGIdf.loc[MapWGIdf["country_code"] == iso, Data["Year"].tolist()[i]]
                float(MapWGIdf.query(f'country_code == "{iso}"')[str(Data["Year"].tolist()[i])].iloc[0])
            )
        except:
            print("The entered year is not available in our database!")
            logging.debug(f"Error while fetching the wgi, the ISO is {iso}")
    try:
        Data["Quantity (kg)"] = [
            float(x) * 1000 for x in Data["Quantity (kg)"].tolist()
        ]
    except:
        print(
            "Error in converting values to float, check the formatting in the Template"
        )
        logging.debug("Excel file not formatted correctly! numerical must be numbers")
        raise ValueError
    try:
        Data["Value (USD)"] = [float(x) * 1000 for x in Data["Value (USD)"].to_list()]
    except:
        print(
            "Error in converting values to float, check the formatting in the Template"
        )
        logging.debug("Excel file not formatted correctly! numerical must be numbers")
        raise ValueError
    Data["partnerISO"] = ISO
    Data["partnerWGI"] = wgi
    Data["cmdCode"] = HS_Code
    Data["reporterDesc"] = ["Company"] * len(ISO)
    Data["reporterISO"] = [999] * len(ISO)

    Data.columns = [
        "Commodity",
        "partnerDesc",
        "qty",
        "cifvalue",
        "period",
        "Notes",
        "partnerISO",
        "partnerWGI",
        "cmdCode",
        "reporterDesc",
        "reporterISO",
    ]
    return Data


########################################################
##   Extrade production data functions --  GeoPolRisk ##
########################################################


def getProd(resource, db):
    Mapdf = db.production["HS Code Map"]
    if resource in Mapdf["ID"].tolist():
        MappedTableName = Mapdf.loc[Mapdf["ID"] == resource, "Sheet_name"]
    elif str(resource) in Mapdf["HS Code"].tolist() and resource != "Not Available":
        MappedTableName = Mapdf.loc[Mapdf["HS Code"] == str(resource), "Sheet_name"]
    else:
        logging.debug(f"Resource {resource} not found in HS Code Map!")
        raise ValueError(f"Resource {resource} not found in HS Code Map!")

    result = db.production[MappedTableName.iloc[0]]
    return result


########################################################
##   Define multiple regions --  GeoPolRisk ##
########################################################


def regions(region_dict, db):
    if "Country_ISO" not in db.production:
        raise KeyError("Country_ISO table is missing in the database. Ensure it is loaded correctly.")

    trackregion = 0
    if region_dict:
        for key, value in region_dict.items():
            if not isinstance(key, str) or not isinstance(value, list):
                logging.debug("Dictionary input to regions does not match required type.")
                return None
            missing_countries = [
                x
                for x in value
                if x not in db.production["Country_ISO"]["Country"].tolist() and
                   x not in db.production["Country_ISO"]["ISO"].tolist()
            ]
            if missing_countries:
                logging.debug(f"Error in creating a region! Missing countries: {missing_countries}")
                return None
            else:
                trackregion += 1
                db.regionslist[key] = value
    if trackregion > 0:
        db.regional = True
    # The function must be called before calling any other functions in the core
    # module. The following lines populate the region list with all the countries
    # in the world including EU defined in the init file.
    for i in db.production["Country_ISO"]["Country"].tolist():
        if i not in db.regionslist:
            db.regionslist[i] = [i]
