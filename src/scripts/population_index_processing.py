import traceback
import pandas as pd

from src.config.manager import ResourceManager # switchboard
from src.scripts.load_to_storage import download_file

SEQUENCE_INDEX_URL = "https://ftp-trace.ncbi.nih.gov/1000genomes/ftp/20131219.populations.tsv"
COL_NAMES = ["population_description", "population_code", "super_population", "dna_from_blood", "offspring_available_from_trios", "pilot_samples", "phase1_samples", "final_phase_samples", "total"]
DTYPES = {
    "population_description": "string",
    "population_code": "string",
    "super_population": "string",
    "dna_from_blood": "boolean",
    "offspring_available_from_trios": "boolean",
    "pilot_samples": "Int64",
    "phase1_samples": "Int64",
    "final_phase_samples": "Int64",
    "total": "Int64"
 }

storage = ResourceManager.get_main_storage()
database = ResourceManager.get_main_db()
input_path = storage.get_abs_path("raw/populations.tsv")

if storage.exists("raw/populations.tsv"):
    print("Population index detected, skipping download...")
else:
    print(f"Population index missing, downloading to {input_path}")
    download_file("raw/populations.tsv", SEQUENCE_INDEX_URL)

if database.exists("population_raw"):
    print("Population index already loaded in database, skipping processing...")
else:
    print("Processing population index and loading to database...")
    population_df = pd.read_csv(
        input_path,
        sep = "\t",
        header = 0,
        names = COL_NAMES,
        dtype = DTYPES,
        usecols = lambda col: col not in ["Total", "total"], # Drop "Total" column if it exists
        true_values = ["yes"],
        false_values = ["no"]
    ).dropna(how="all")
    
    # Drop "Total" row if it exists
    population_df = population_df[population_df["population_description"] != "Total"]

    database.load_data(population_df, "population_raw")
    