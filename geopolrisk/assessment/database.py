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
import time
from tqdm import tqdm
from datetime import datetime
from pathlib import Path
import sys

logging = logging



def execute_query(query, db_path="", params=None, retries=5, delay=0.1):
    """
    Execute an SQL query on a SQLite database with retry mechanism for locked database.

    Args:
        query (str): SQL query to execute.
        db_path (str): Path to the SQLite database file.
        params (tuple or list, optional): Parameters for the SQL query.
        retries (int, optional): Number of retries if the database is locked. Default is 5.
        delay (float, optional): Delay between retries in seconds. Default is 0.1 seconds.

    Returns:
        list or None: Query results for SELECT queries, None for others.
    """
    attempts = 0
    while attempts <= retries:
        try:
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

            break  # Exit loop if query succeeds

        except sqlite3.OperationalError as e:
            if "database is locked" in str(e).lower():
                attempts += 1
                if attempts > retries:
                    raise Exception(
                        f"Database is locked after {retries} retries."
                    ) from e
                time.sleep(delay)  # Wait before retrying
            else:
                raise  # Raise other operational errors immediately


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
        "Steam Coal",
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
        try:
            if not Path(db).exists():
                raise FileNotFoundError(f"Database file {db} not found!")

            conn = sqlite3.connect(db)
            cursor = conn.cursor()
            cursor.execute("SELECT name FROM sqlite_master WHERE type='table';")
            result = cursor.fetchall()
            existing_tables = [row[0] for row in result]
            conn.close()

            missing_tables = [table for table in table_names if table not in existing_tables]
            if missing_tables:
                raise FileNotFoundError(f"Missing tables: {missing_tables}")
            return True
        except Exception as e:
            print(f"Unable to verify tables in database {db}: {e}")
            raise FileNotFoundError

    ###############################################
    ## Extracting TABLES FROM THE DATABASE FILES ##
    ###############################################

    def extract_tables_to_df(self, db_path, table_names):
        tables = {}
        try:
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
                                -- where bacitab.k IN ('760110', '260400') -- only for a better test performance
                            """
                else:
                    query = f"SELECT * FROM '{table_name}'"
                table_df = pd.read_sql_query(query, conn)
                tables[table_name] = table_df
            conn.close()
        except Exception as e:
            print(f"Error to read tables {table_names} from database {db_path} - {e}")
            conn.close()
        return tables

    def load_databases(self):
        db_paths = {
            "world_mining_data": self._dwmd,
            "wgi": self._dwgi,
            "baci": self._dbaci,
        }

        for name, path in db_paths.items():
            if name == "world_mining_data" and self.check_db_tables(path, self.Tables_world_mining_data):
                tables = self.extract_tables_to_df(path, self.Tables_world_mining_data)
                self.production = tables
                if "HS Code Map" in self.production:
                    self.production["HS Code Map"] = (
                        self.production["HS Code Map"]
                        .loc[self.production["HS Code Map"]["HS Code"] != "Not Available"]
                        .dropna(subset=["Symbol"])
                        .dropna(subset=["Symbol"])
                    )

            elif name == "wgi" and self.check_db_tables(path, self.Tables_wgi):
                tables = self.extract_tables_to_df(path, self.Tables_wgi)
                self.wgi = tables.get("Normalized")

            elif name == "baci" and self.check_db_tables(path, self.Tables_baci):
                tables = self.extract_tables_to_df(path, self.Tables_baci)
                self.baci_trade = tables.get("baci_trade")

            else:
                raise FileNotFoundError(f"Error loading database: {name} at {path}")

    def define_default_regions(self):
        self.regionslist["EU"] = [
            "Austria",
            "Belgium",
            "Bulgaria",
            "Croatia",
            "Cyprus",
            "Czechia",
            "Denmark",
            "Estonia",
            "Finland",
            "France",
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
        self.regional = True



    # Logging config
    Filename = f"Log_File_{datetime.now():%Y-%m-%d(%H-%M-%S)}.log"
    log_path = Path.cwd() / "output" / Filename
    log_path.parent.mkdir(exist_ok=True)

    try:
        logging.basicConfig(
            level=logging.DEBUG,
            format="%(asctime)s | %(levelname)s | %(message)s",
            filename=str(log_path),
            filemode="w",
        )
    except Exception:
        print("Cannot create log file!")
