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


import sqlite3
import pandas as pd
import logging
import os
from tqdm import tqdm
from datetime import datetime
import importlib.resources
from pathlib import Path
import sys

logging = logging


# Generic SQL function (multi use)
def execute_query(query, db_path="", params=None):
    conn = sqlite3.connect(db_path)
    cursor = conn.cursor()
    is_select_query = query.strip().lower().startswith("select")
    if is_select_query:
        cursor.execute(query, params or [])
        results = cursor.fetchall()
    else:
        cursor.execute(query, params or [])
        results = None
    conn.commit()
    conn.close()
    if is_select_query:
        return results


class Database:
    def __init__(self):
        """
        Initialize the folders and verify database files exist.
        Outputs, including `Datarecords.db`, are saved in an `output` folder in the current working directory.
        """
        try:
            self.geopolrisk_root = Path(sys.modules["geopolrisk"].__file__).parent

            self._dwmd = str(self.geopolrisk_root / "lib" / "world_mining_data.db")
            self._dwgi = str(self.geopolrisk_root / "lib" / "wgi.db")
            self._dbaci = str(self.geopolrisk_root / "lib" / "baci.db")

            self.output_directory = Path.cwd() / "output"
            self.output_directory.mkdir(exist_ok=True)
            self.output_file = str(self.output_directory / "Datarecords.db")

            # Verify database files exist
            for db_path in [self._dwmd, self._dwgi, self._dbaci]:
                if not Path(db_path).is_file():
                    raise FileNotFoundError(f"Database file {db_path} not found!")

            self.production = {}
            self.baci_trade = None
            self.wgi = None
            self.regionslist = {}
            self.regional = False

        except Exception as e:
            print(f"Error during initialization: {e}")
            raise FileNotFoundError("Initialization failed due to missing directories or files.")

    ##############################################
    ##   READING TABLES FROM THE DATABASE FILES ##
    ##############################################

    Tables_world_mining_data = [
        "Aluminium",
        "Antimony",
        "Arsenic",
        "Asbestos",
        "Baryte",
        "Bauxite",
        "Bentonite",
        "Beryllium (conc.)",
        "Bismuth",
        "Boron Minerals",
        "Cadmium",
        "Chromium (Cr2O3)",
        "Cobalt",
        "Coking Coal",
        "Copper",
        "Diamonds (Gem)",
        "Diamonds (Ind)",
        "Diatomite",
        "Feldspar",
        "Fluorspar",
        "Gallium",
        "Germanium",
        "Gold",
        "Graphite",
        "Gypsum and Anhydrite",
        "Indium",
        "Iron (Fe)",
        "Kaolin (China-Clay)",
        "Lead",
        "Lignite",
        "Lithium (Li2O)",
        "logging",
        "Magnesite",
        "Manganese",
        "Mercury",
        "Molybdenum",
        "Natural Gas",
        "Nickel",
        "Niobium (Nb2O5)",
        "Oil Sands (part of Petroleum)",
        "Oil Shales",
        "Palladium",
        "Perlite",
        "Petroleum",
        "Phosphate Rock (P2O5)",
        "Platinum",
        "Potash (K2O)",
        "Rare Earths (REO)",
        "Rhenium",
        "Rhodium",
        "Salt (rock, brines, marine)",
        "Selenium",
        "Silver",
        "Steam Coal ",
        "Sulfur (elementar & industrial)",
        "Talc, Steatite & Pyrophyllite",
        "Tantalum (Ta2O5)",
        "Tellurium",
        "Tin",
        "Titanium (TiO2)",
        "Tungsten (W)",
        "Uranium (U3O8)",
        "Vanadium (V)",
        "Vermiculite",
        "Zinc",
        "Zircon",
        "Country_ISO",
        "HS Code Map",
    ]
    Tables_wgi = [
        "Normalized",
    ]
    Tables_baci = [
        "baci_trade",
    ]

    # Function to check if database exists and fetch the required tables
    def check_db_tables(self, db, table_names):
        """
        Check if the required tables exist in the database.
        """
        try:
            print(f"Attempting to connect to database: {db}")  ### DEBUG
            if not Path(db).exists():
                print(f"File does not exist: {db}")  ### DEBUG
                raise FileNotFoundError(f"Database file {db} not found!")

            conn = sqlite3.connect(db)
            cursor = conn.cursor()
            query = "SELECT name FROM sqlite_master WHERE type='table';"
            cursor.execute(query)
            result = cursor.fetchall()
            existing_tables = [row[0] for row in result]
            conn.close()

            # print(f"Tables found in database {db}: {existing_tables}")  ### DEBUG

            # Check for missing tables
            missing_tables = [table for table in table_names if table not in existing_tables]
            if missing_tables:
                print(f"Missing tables: {missing_tables}")  ### DEBUG
                raise FileNotFoundError(f"Missing tables: {missing_tables}")
            return True
        except Exception as e:
            print(f"Unable to verify tables in database {db}: {e}")  ### DEBUG
            raise FileNotFoundError

    ###############################################
    ## Extracting TABLES FROM THE DATABASE FILES ##
    ###############################################

    def initialize(self):
        self.load_databases()
        self.define_default_regions()

    def extract_tables_to_df(self, db_path, table_names):
        try:
            tables = {}
            conn = sqlite3.connect(db_path)
            for table_name in tqdm(
                table_names,
                desc=f"Reading table/s {table_names} from the library database {db_path}.",
            ):
                if table_name == "baci_trade":
                    query = f"""
                            select 
                                bacitab.t as period,
                                bacitab.j as reporterCode,
                                (SELECT cc.country_name FROM country_codes_V202401b cc WHERE bacitab.j = cc.country_code) AS reporterDesc,
                                (SELECT cc.country_iso3 FROM country_codes_V202401b cc WHERE bacitab.j = cc.country_code) AS reporterISO,
                                bacitab.i as partnerCode,
                                (SELECT cc.country_name FROM country_codes_V202401b cc WHERE bacitab.i = cc.country_code) AS partnerDesc,
                                (SELECT cc.country_iso3 FROM country_codes_V202401b cc WHERE bacitab.i = cc.country_code) AS partnerISO,
                                bacitab.k as cmdCode,
                                REPLACE(TRIM(bacitab.q), 'NA', 0) as qty,
	                            REPLACE(TRIM(bacitab.v),'NA', 0) as cifvalue,
                                (SELECT vwyc.wgi FROM v_wgi_year_country vwyc WHERE bacitab.t = vwyc.Year and bacitab.i = vwyc.country_code) AS partnerWGI
                            from baci_trade bacitab
                              --where bacitab.k = '260400'
                            """
                    # Test-Query - read the vieww
                    # query = f"""
                    #         select 
                    #         *
                    #         from v_baci_trade_with_wgi bacitab
                    #         --where cmdCode = '260400'
                    #         """
                else:
                    query = f"SELECT * FROM '{table_name}'"

                try:
                    # print(f"Querying table: {table_name}")  # Debug
                    table_df = pd.read_sql_query(query, conn)
                    # print(f"Loaded table: {table_name}, {len(table_df)} rows")  # Debug
                    tables[table_name] = table_df
                except Exception as e:
                    print(f"Error reading table {table_name}: {e}")  # Debug
                    logging.debug(f"Error reading table {table_name}: {e}")
            conn.close()
        except Exception as e:
            print(f"Error to read tables {table_names} from database {db_path} - {e}")
            conn.close()
        return tables

    def load_databases(self):
        """
        Load all necessary databases and return the extracted tables.
        """
        db_paths = {
            "world_mining_data": self._dwmd,
            "wgi": self._dwgi,
            "baci": self._dbaci,
        }

        for name, path in db_paths.items():
            if name == "world_mining_data" and self.check_db_tables(path, self.Tables_world_mining_data):
                tables = self.extract_tables_to_df(path, self.Tables_world_mining_data)
                self.production = tables  # Store all tables in production
                # print(f"Loaded tables: {self.production.keys()}")
            elif name == "wgi" and self.check_db_tables(path, self.Tables_wgi):
                tables = self.extract_tables_to_df(path, self.Tables_wgi)
                # print(f"Extracted tables from wgi.db: {tables.keys()}")  # Debug message
                if "Normalized" in tables:
                    self.wgi = tables["Normalized"]
                else:
                    print("Table 'Normalized' not found in wgi.db")  # Warning message
                    self.wgi = None
            elif name == "baci" and self.check_db_tables(path, self.Tables_baci):
                tables = self.extract_tables_to_df(path, self.Tables_baci)
                self.baci_trade = tables["baci_trade"]  # Extract only the required table
            else:
                raise FileNotFoundError(f"Error loading database: {name} at {path}")

    #############################################################
    ## Extracting the dataframes into the individual variables ##
    #############################################################


    def define_default_regions(self):
        self.regionslist['EU'] = [
        "Austria",
        "Belgium",
        "Belgium-Luxembourg",
        "Bulgaria",
        "Croatia",
        "Czechia",
        "Czechoslovakia",
        "Denmark",
        "Estonia",
        "Finland",
        "France",
        "Fmr Dem. Rep. of Germany",
        "Fmr Fed. Rep. of Germany",
        "Germany",
        "Greece",
        "Hungary",
        "Ireland",
        "Italy",
        "Latvia",
        "Lithuania",
        "Luxembourg",
        "Malta",
        "Netherlands",
        "Poland",
        "Portugal",
        "Romania",
        "Slovakia",
        "Slovenia",
        "Spain",
        "Sweden",
    ]


###########################################################
## Creating a log object and file for logging the errors ##
###########################################################

Filename = "Log_File_{:%Y-%m-%d(%H-%M-%S)}.log".format(datetime.now())
log_level = logging.DEBUG
log_path = Path.cwd() / "output" / Filename
log_path.parent.mkdir(exist_ok=True)  # Ensure the output directory exists

try:
    logging.basicConfig(
        level=log_level,
        format="""%(asctime)s | %(levelname)s | %(threadName)-10s |
          %(filename)s:%(lineno)s - %(funcName)20s() |
            %(message)s""",
        filename=str(log_path),
        filemode="w",
    )
except:
    print("Cannot create log file!")
