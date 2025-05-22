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
from pathlib import Path
from .database import logging, execute_query


tradepath = None

########################################
##   Utility functions --  GeoPolRisk ##
########################################


def replace_func(x):
    if pd.isna(x) or x in [
        None,
        "NA",
        " ",
        "",
    ]:
        return 0
    return x


def format_value(df, col, value):
    """function that returns the value with or without quotation marks depending on the column type"""
    if df.dtypes[col] == "object":
        return f"'{value}'"
    else:
        return str(value)


def cvtcountry(db, country, type="ISO"):
    # Function to convert country inputs, ISO to name or name to ISO
    """
    Type can be either 'ISO' or 'Name'
    Convert between country name and ISO code.
    - `type="ISO"`: Convert country name to ISO.
    - `type="Name"`: Convert ISO to country name.
    """
    MapISOdf = db.production["Country_ISO"]
    MapISOdf["ISO"] = MapISOdf["ISO"].astype(int)

    if type == "ISO":
        if country in MapISOdf["Country"].tolist():
            result = MapISOdf.loc[MapISOdf["Country"] == country, "ISO"].iloc[0]
            return result
        elif country in MapISOdf["ISO"].astype(int).tolist():
            return country
        elif db.regional and country in db.regionslist:
            if country in MapISOdf["Country"].values:
                result = MapISOdf.loc[MapISOdf["Country"] == country, "ISO"].values[0]
                return result
            elif isinstance(country, int) and country in MapISOdf["ISO"].values:
                return country
        else:
            logging.debug(f"To int: Entered country '{country}' does not exist in our database!")
            raise ValueError

    elif type == "Name":
        if country in MapISOdf["ISO"].astype(int).tolist():
            result = MapISOdf.loc[MapISOdf["ISO"] == country, "Country"].iloc[0]
            return result
        elif str(country) in MapISOdf["Country"].tolist():
            return country
        elif db.regional and country in db.regionslist:
            if isinstance(country, int) and country in MapISOdf["ISO"].values:
                result = MapISOdf.loc[MapISOdf["ISO"] == country, "Country"].values[0]
                return result
            elif country in MapISOdf["Country"].values:
                return country
        else:
            logging.debug(f"To Name: Entered country '{country}' does not exist in our database!")
            raise ValueError

    if db.regional and country in db.regionslist:
        return country

    logging.debug(f"Entered country '{country}' does not exist in our database!")
    raise ValueError(f"Country '{country}' not found.")


def sumproduct(A: list, B: list):
    return sum(i * j for i, j in zip(A, B))


def create_id(HS, ISO, Year):
    return str(HS) + str(ISO) + str(Year)


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
        	"Price"	REAL,
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
        exists = execute_query(check_query, db_path=dbpath, params=(row["DBID"],))

        if exists:
            update_query = """
                UPDATE recordData
                SET 
                    "Country [Economic Entity]" = ?,
                    "Raw Material" = ?,
                    "Year" = ?,
                    "GeoPolRisk Score" = ?,
                    "GeoPolRisk Characterization Factor [eq. Kg-Cu/Kg]" = ?,
                    "HHI" = ?,
                    "Import Risk" = ?,
                    "Price" = ?
                WHERE DBID = ?;
            """
            params = (
                row["Country [Economic Entity]"],
                row["Raw Material"],
                row["Year"],
                row["GeoPolRisk Score"],
                row["GeoPolRisk Characterization Factor [eq. Kg-Cu/Kg]"],
                row["HHI"],
                row["Import Risk"],
                row["Price"],
                row["DBID"],
            )
            try:
                execute_query(update_query, db_path=dbpath, params=params)
            except Exception as e:
                print(
                    "Failed to write to output database, Check logs! - Update failed!"
                )
                logging.debug(
                    f"Failed to write to output database - Update Query - Dataframe index = {index} | {update_query} | {row} | Error = {e}"
                )
        else:
            insert_query = """
                INSERT INTO recordData (
                    DBID,
                    "Country [Economic Entity]",
                    "Raw Material",
                    "Year",
                    "GeoPolRisk Score",
                    "GeoPolRisk Characterization Factor [eq. Kg-Cu/Kg]",
                    "HHI",
                    "Import Risk",
                    "Price"
                ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?);
            """
            params = (
                row["DBID"],
                row["Country [Economic Entity]"],
                row["Raw Material"],
                row["Year"],
                row["GeoPolRisk Score"],
                row["GeoPolRisk Characterization Factor [eq. Kg-Cu/Kg]"],
                row["HHI"],
                row["Import Risk"],
                row["Price"],
            )
            try:
                execute_query(insert_query, db_path=dbpath, params=params)
            except Exception as e:
                print(
                    "Failed to write to output database, Check logs! - Insert failed!"
                )
                logging.debug(
                    f"Failed to write to output database - Insert Query - Dataframe index = {index} | {update_query} | {row} | Error = {e}"
                )


###################################################
##   Extrade trade data functions --  GeoPolRisk ##
###################################################


def getbacidata(period: int, country: int, rawmaterial: str, data):
    """
    get the baci-trade-data from the baci_trade dataframe
    """
    try:
        df_query = f"(period == {period}) & (reporterCode == {country}) & (rawMaterial == '{rawmaterial}')"
        baci_data = data.query(df_query)
    except Exception as e:
        logging.debug(
            f"Error when querying baci database - issue with -- {e} - Data: {country}, {rawmaterial}, {period}"
        )
        return None
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
    'Raw Material' -> The Reference name of the commodity traded
    """
    if baci_data is None or isinstance(baci_data, type(None)) or len(baci_data) == 0:
        logging.debug(
            f"baci data error: - {baci_data} - period == {period} - reporterCode == {country} - Raw Material == '{rawmaterial}'"
        )
        return None
    else:
        baci_data.loc[:, "qty"] = baci_data["qty"].apply(replace_func).astype(float)
        baci_data.loc[:, "cifvalue"] = (
            baci_data["cifvalue"].apply(replace_func).astype(float)
        )
        return baci_data


def aggregateTrade(period: int, country: list, rawmaterial: str, data):
    """
    The function is only to aggregate the trade for each partner country in the region.
    """

    def wgi_func(x):
        if isinstance(x, float):
            return x
        else:
            if x is None or isinstance(x, type(None)) or x.strip() == "NA":
                return 0.5
            else:
                return x

    SUMQTY, SUMVAL, SUMNUM = [], [], []
    pISO = []
    for n in country:
        pISO.append(int(cvtcountry(n, type="ISO")))
    logging.info(f"The partner list is {pISO}")
    for i, n in enumerate(country):
        try:
            baci_data = getbacidata(
                period, cvtcountry(n, type="ISO"), rawmaterial, data
            )
        except Exception as e:
            logging.debug(f"Error when filtering database - getbacidata function + {e}")
        if baci_data is None:
            QTY, WGI, VAL = [0], [0], [0]
        else:
            tradedata = baci_data.copy()
            logging.debug(tradedata["partnerCode"].tolist())
            tradedata.loc[tradedata["partnerCode"].isin(pISO), "partnerWGI"] = 0.00
            logging.debug(tradedata)
            QTY = tradedata["qty"].tolist()
            WGI = tradedata["partnerWGI"].apply(wgi_func).astype(float).tolist()
            VAL = tradedata["cifvalue"].tolist()
        SUMQTY.append(sum(QTY))
        SUMVAL.append(sum(VAL))
        SUMNUM.append(sumproduct(QTY, WGI))

    if sum(SUMQTY) == 0:
        logging.debug(
            f"Total trade for the region is 0, Country: {country}, Raw Material: {rawmaterial}, Year: {period}"
        )
        Price = 0
    else:
        Price = sum(SUMVAL) / sum(SUMQTY)
    """
    The function returns the numerator of the import risk, 
    The total trade for the region,
    The price calculated with the total value for all the countries in the region &
    the total quantity traded with the countries in the region.
    """
    return sum(SUMNUM), sum(SUMQTY), Price


###########################################################
## Converting company trade data into a usable dataframe ##
###########################################################


def transformdata(db, mode="prod"):
    def cvtresource():
        pass

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
    HS_Code = Data["Metal"].tolist()
    ISO = []
    for country in Data["Country of Origin"].tolist():
        ISO.append(cvtcountry(country, type="ISO"))
    MapWGIdf = db.wgi
    wgi = []
    for i, iso in enumerate(ISO):
        try:
            wgi.append(
                # MapWGIdf.loc[MapWGIdf["country_code"] == iso, Data["Year"].tolist()[i]]
                float(
                    MapWGIdf.query(f'country_code == "{iso}"')[
                        str(Data["Year"].tolist()[i])
                    ].iloc[0]
                )
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
        "rawMaterial",
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


def getProd(rawmaterial, db):
    """
    The dictionary have a unique identifier that is accessed through a table called 'HS Code Map'
    The mapping table has the following structure
    'Sheet_name' -> The name of the table corresponding to the raw material
    'Category - WMD' -> The world mining data categorization of the raw material
    'ID' -> The name of the raw material (metals and minerals)
    'HS Code' -> The HS Code mapping of raw material
    'Description' -> The description of the HS code
    'Symbol' -> Element equivalent to the raw material
    """
    Mapdf = db.production["HS Code Map"]
    if rawmaterial in Mapdf["Reference ID"].tolist():
        MappedTableName = Mapdf.loc[Mapdf["Reference ID"] == rawmaterial, "Sheet_name"]
    else:
        print("Entered raw material does not exist in our database!")
        logging.debug(
            f"Error while fetching raw material. Entered raw material = {rawmaterial}"
        )
        raise ValueError

    """
    The returned dataframe is mapped based on the input raw material.
    The output dataframe has the following structure.
    'Country' -> The country name, according to the BACI
    'Country_Code' -> The numeric ISO country code
    'Country_ISO' -> The ISO code of the country
    '2018', '2019', '2020', '2021', '2022' -> The production quantity of each raw material
    'unit' -> The units of the value
    'data_source' -> The data source of each value point
    """

    result = db.production[MappedTableName.iloc[0]]
    return result


########################################################
##   Define multiple regions --  GeoPolRisk ##
########################################################


def regions(region_dict: dict, db):
    """
    Assign user-defined regional groupings to the Database instance.

    Parameters:
    - region_dict: dict, e.g. {"EU": ["France", "Germany"]}
    - db: Database instance, which contains db.production["Country_ISO"]
    """
    if not isinstance(region_dict, dict):
        raise TypeError("region_dict must be a dictionary")

    valid_countries = db.production.get("Country_ISO", pd.DataFrame())
    valid_names = valid_countries["Country"].astype(str).tolist()
    valid_isos = valid_countries["ISO"].astype(str).tolist()

    for region_name, members in region_dict.items():
        if not isinstance(region_name, str) or not isinstance(members, list):
            raise ValueError(f"Invalid region format: {region_name} → {members}")

        not_found = [
            c for c in members
            if str(c) not in valid_names and str(c) not in valid_isos
        ]
        if not_found:
            raise ValueError(
                f"The following countries in region '{region_name}' were not found in Country_ISO: {not_found}"
            )

        db.regionslist[region_name] = members

    for name in valid_names:
        db.regionslist[name] = [name]

    db.regional = True


########################################################
##   Mapping Functions - GeoPolRisk ##
########################################################


def Mapping(db):
    """
    Creates a dictionary mapping 'Reference ID' to a list of HS Codes.
    Extracts data from 'HS Code Map' in 'databases.production' and ensures data validity.
    Returns an empty dictionary in case of errors.
    """
    try:
        temp = db.production.get("HS Code Map")

        if temp is None or temp.empty:
            logging.debug("HS Code Map dataset is empty or missing.")
            return {}
        hs_map = {}

        for _, row in temp.iterrows():
            try:
                hs_codes = [int(row["HS Code"])]

                if (
                    pd.notna(row["HS Code - Complementary"])
                    and row["HS Code - Complementary"]
                ):
                    hs_codes.extend(
                        [
                            int(code)
                            for code in row["HS Code - Complementary"].split(";")
                        ]
                    )

                hs_map[row["Reference ID"]] = list(set(hs_codes))
            except (ValueError, KeyError) as e:
                logging.debug(f"Skipping row - {row} due to data error: {e}")

        return hs_map

    except KeyError as e:
        logging.debug(f"Missing required column: {e}")
    except ValueError as e:
        logging.debug(f"Data validation error: {e}")
    except Exception as e:
        logging.debug(f"Unexpected error: {e}")

    return {}


def mapped_baci(db):
    """
    This function processes trade data by mapping commodity codes to raw materials.
    It aggregates trade information (such as quantities and CIF values) for each raw material,
    while handling cases where multiple commodity codes exist for a raw material.
    The function will group trade data by raw material, period, and other relevant fields,
    summing quantities and CIF values, and concatenating commodity codes where applicable.
    """
    try:
        hs_map = Mapping(db)
        master_data = []

        for raw_material, codes in hs_map.items():
            temp = db.baci_trade
            filtered_data = temp[temp["cmdCode"].astype(str).isin(map(str, codes))].copy()

            if filtered_data.empty:
                continue

            filtered_data["rawMaterial"] = raw_material
            filtered_data["cmdCode"] = filtered_data["cmdCode"].astype(str)
            filtered_data["period"] = filtered_data["period"].astype(str)
            filtered_data["reporterCode"] = filtered_data["reporterCode"].astype(str)
            filtered_data["partnerCode"] = filtered_data["partnerCode"].astype(str)
            filtered_data["qty"] = pd.to_numeric(filtered_data["qty"], errors="coerce")
            filtered_data["cifvalue"] = pd.to_numeric(filtered_data["cifvalue"], errors="coerce")

            grouped_data = filtered_data.groupby(
                [
                    "period",
                    "reporterCode",
                    "reporterDesc",
                    "reporterISO",
                    "partnerCode",
                    "partnerDesc",
                    "partnerISO",
                    "rawMaterial",
                ],
                as_index=False,
            ).agg(
                {
                    "cmdCode": lambda x: ";".join(sorted(set(x))),
                    "qty": "sum",
                    "cifvalue": "sum",
                    "partnerWGI": "first",
                }
            )

            master_data.append(grouped_data)

        if master_data:
            return pd.concat(master_data, ignore_index=True)
        else:
            return pd.DataFrame()  # fallback if nothing matched

    except Exception as e:
        logging.debug(f"Error in mapped_baci function: {e}")
        raise


def default_rmlist(db):
    hs_map = Mapping(db)
    return list(hs_map.keys())
