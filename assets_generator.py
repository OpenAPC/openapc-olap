#!/usr/bin/env python3
# -*- coding: UTF-8 -*-

import argparse
import csv
import configparser
from copy import deepcopy
import json
import os
import sys

from util import colorise
import springer_compact_coverage as scc

import sqlalchemy

ARG_HELP_STRINGS = {

    "dir": "A path to a directory where the generated output files should be stored. " +
           "If omitted, output will be written to the current directory.",
    "num_api_lookups": "Stop execution after n journal lookups " +
                       "when performing the coverage_stats job. Useful for " +
                       "reducing API loads and saving results from time to time.",
    "refetch": "Try to re-fetch a journal csv file from Springerlink during the " +
               "coverage_stats job when a DOI is not found. Only useful if the journal csv " +
               "directory has not been cleared recently."
}

APC_DE_FILE = "../openapc-de/data/apc_de.csv"
BPC_FILE = "../openapc-de/data/bpc.csv"
TRANSFORMATIVE_AGREEMENTS_FILE = "../openapc-de/data/transformative_agreements/transformative_agreements.csv"
DEAL_WILEY_OPT_OUT_FILE = "../openapc-de/data/transformative_agreements/deal_germany_opt_out/deal_wiley_germany_opt_out.csv"
DEAL_SPRINGER_OPT_OUT_FILE = "../openapc-de/data/transformative_agreements/deal_germany_opt_out/deal_springer_nature_germany_opt_out.csv"
INSTITUTIONS_FILE = "../openapc-de/data/institutions.csv"
INSTITUTIONS_TRANSFORMATIVE_AGREEMENTS_FILE = "../openapc-de/data/institutions_transformative_agreements.csv"
INSTITUTIONS_BPC_FILE = "../openapc-de/data/institutions_bpcs.csv"

DEAL_IMPRINTS = {
    "Wiley-Blackwell": ["Wiley-Blackwell", "EMBO", "American Geophysical Union (AGU)", "International Union of Crystallography (IUCr)", "The Econometric Society"],
    "Springer Nature": ["Springer Nature", "Zhejiang University Press"]
}

def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("job", choices=["tables", "model", "yamls", "db_settings",
                                        "coverage_stats"])
    parser.add_argument("-d", "--dir", help=ARG_HELP_STRINGS["dir"])
    parser.add_argument("-n", "--num_api_lookups", type=int,
                        help=ARG_HELP_STRINGS["num_api_lookups"])
    parser.add_argument("--refetch", action="store_true",
                        help=ARG_HELP_STRINGS["refetch"])
    args = parser.parse_args()

    path = "."
    if args.dir:
        if os.path.isdir(args.dir):
            path = args.dir
        else:
            print("ERROR: '" + args.dir + "' is no valid directory!")

    if args.job == "tables":
        if not os.path.isfile("db_settings.ini"):
            print("ERROR: Database Configuration file db_settings.ini not found!")
            sys.exit()
        cparser = configparser.ConfigParser()
        cparser.read("db_settings.ini")
        try:
            db_user = cparser.get("postgres_credentials", "user")
            db_pass = cparser.get("postgres_credentials", "pass")
        except (configparser.NoSectionError, configparser.NoOptionError) as e:
            print("ERROR: db_settings.ini is malformed ({})".format(e.message))
            sys.exit()
        psql_uri = "postgresql://" + db_user + ":" + db_pass + "@localhost/openapc_db"
        engine = sqlalchemy.create_engine(psql_uri)
        create_cubes_tables(engine, APC_DE_FILE, TRANSFORMATIVE_AGREEMENTS_FILE)
        with engine.begin() as connection:
            connection.execute("GRANT SELECT ON ALL TABLES IN SCHEMA openapc_schema TO cubes_user")


    elif args.job == "model":
        generate_model_file(path)
    elif args.job == "yamls":
        generate_yamls(path)
    elif args.job == "db_settings":
        if os.path.isfile("db_settings.ini"):
            print("ERROR: db_settings.ini already exists")
            sys.exit()
        cparser = configparser.ConfigParser()
        cparser.add_section('postgres_credentials')
        cparser.set('postgres_credentials', 'USER', 'table_creator')
        cparser.set('postgres_credentials', 'PASS', 'change_me')
        with open('db_settings.ini', 'w') as config_file:
            cparser.write(config_file)
    elif args.job == "coverage_stats":
        scc.update_coverage_stats(TRANSFORMATIVE_AGREEMENTS_FILE, args.num_api_lookups, args.refetch)



def init_table(table, fields, create_id=False):

    type_map = {"integer": sqlalchemy.Integer,
                "float": sqlalchemy.Numeric,
                "string": sqlalchemy.String(512),
                "text": sqlalchemy.Text,
                "date": sqlalchemy.Text,
                "boolean": sqlalchemy.Integer}

    if create_id:
        col = sqlalchemy.schema.Column('id', sqlalchemy.Integer, primary_key=True)
        table.append_column(col)

    for (field_name, field_type) in fields:
        col = sqlalchemy.schema.Column(field_name, type_map[field_type.lower()])
        table.append_column(col)

    table.create()

def create_cubes_tables(connectable, apc_file_name, transformative_agreements_file_name, schema="openapc_schema"):

    apc_fields = [
        ("institution", "string"),
        ("period", "string"),
        ("euro", "float"),
        ("doi", "string"),
        ("is_hybrid", "string"),
        ("publisher", "string"),
        ("journal_full_title", "string"),
        ("issn", "string"),
        ("issn_print", "string"),
        ("issn_electronic", "string"),
        ("issn_l", "string"),
        ("license_ref", "string"),
        ("indexed_in_crossref", "string"),
        ("pmid", "string"),
        ("pmcid", "string"),
        ("ut", "string"),
        ("url", "string"),
        ("doaj", "string"),
        ("country", "string"),
        ("institution_ror", "string")
    ]

    deal_fields = apc_fields + [("opt_out", "string")]

    transformative_agreements_fields = [
        ("institution", "string"),
        ("period", "string"),
        ("doi", "string"),
        ("is_hybrid", "string"),
        ("publisher", "string"),
        ("journal_full_title", "string"),
        ("issn", "string"),
        ("issn_print", "string"),
        ("issn_electronic", "string"),
        ("issn_l", "string"),
        ("license_ref", "string"),
        ("indexed_in_crossref", "string"),
        ("pmid", "string"),
        ("pmcid", "string"),
        ("ut", "string"),
        ("url", "string"),
        ("doaj", "string"),
        ("country", "string"),
        ("agreement", "string")
    ]

    bpc_fields = [
        ("institution", "string"),
        ("period", "string"),
        ("euro", "float"),
        ("doi", "string"),
        ("backlist_oa", "string"),
        ("publisher", "string"),
        ("book_title", "string"),
        ("isbn", "string"),
        ("isbn_print", "string"),
        ("isbn_electronic", "string"),
        ("license_ref", "string"),
        ("indexed_in_crossref", "string"),
        ("doab", "string"),
        ("country", "string")
    ]

    springer_compact_coverage_fields = [
        ("period", "string"),
        ("publisher", "string"),
        ("journal_full_title", "string"),
        ("is_hybrid", "string"),
        ("num_springer_compact_articles", "float"),
        ("num_journal_total_articles", "float"),
        ("num_journal_oa_articles", "float")
    ]

    metadata = sqlalchemy.MetaData(bind=connectable)

    # a dict to store individual insert commands and data for every table
    tables_insert_data = {
        "openapc": {
            "fields": apc_fields,
            "cubes_name": "openapc",
            "data": []
        },
        "transformative_agreements": {
            "fields": transformative_agreements_fields,
            "cubes_name": "transformative_agreements",
            "data": []
        },
        "bpc": {
            "fields": bpc_fields,
            "cubes_name": "bpc",
            "data": []
        },
        "combined": {
            "fields": apc_fields,
            "cubes_name": "combined",
            "data": []
        },
        "springer_compact_coverage": {
             "fields": springer_compact_coverage_fields,
             "cubes_name": "springer_compact_coverage",
             "data": []
        },
        "deal": {
            "fields": deal_fields,
            "cubes_name": "deal",
            "data": []
        }
    }
    
    bpcs_institution_countries = {}

    reader = csv.DictReader(open(INSTITUTIONS_BPC_FILE, "r"))
    for row in reader:
        institution_name = row["institution"]
        country = row["country"]
        bpcs_institution_countries[institution_name] = country

    print(colorise("Processing BPC file...", "green"))
    reader = csv.DictReader(open(BPC_FILE, "r"))
    bpc_data = []
    for row in reader:
        row["book_title"] = row["book_title"].replace(":", "")
        institution = row["institution"]
        row["country"] = bpcs_institution_countries[institution]
        bpc_data.append(row)

    transformative_agreements_institution_countries = {}

    reader = csv.DictReader(open(INSTITUTIONS_TRANSFORMATIVE_AGREEMENTS_FILE, "r"))
    for row in reader:
        institution_name = row["institution"]
        country = row["country"]
        transformative_agreements_institution_countries[institution_name] = country

    journal_coverage = None
    article_pubyears = None
    try:
        cache_file = open(scc.COVERAGE_CACHE_FILE, "r")
        journal_coverage = json.loads(cache_file.read())
        cache_file.close()
        cache_file = open(scc.PUBDATES_CACHE_FILE, "r")
        article_pubyears = json.loads(cache_file.read())
        cache_file.close()
    except IOError as ioe:
        msg = "Error while trying to access cache file: {}"
        print(msg.format(ioe))
        sys.exit()
    except ValueError as ve:
        msg = "Error while trying to decode cache structure in: {}"
        print(msg.format(str(ve)))
        sys.exit()

    summarised_transformative_agreements = {}

    journal_id_title_map = {}

    institution_key_errors = []

    reader = csv.DictReader(open(DEAL_WILEY_OPT_OUT_FILE, "r"))
    print(colorise("Processing Wiley Opt-Out file...", "green"))
    for row in reader:
        row_copy = deepcopy(row) # work on a deep copy since we make some DEAL-specific changes 
        row_copy["opt_out"] = "TRUE"
        if row_copy["publisher"] in DEAL_IMPRINTS["Wiley-Blackwell"]:
            row_copy["publisher"] = "Wiley-Blackwell"
        institution = row_copy["institution"]
        try:
            row_copy["country"] = transformative_agreements_institution_countries[institution]
        except KeyError:
            if institution not in institution_key_errors:
                institution_key_errors.append(institution)
        if row_copy["period"] == "2019":
            # Special rule: Half 2019 costs since DEAL only started in 07/19
            halved = round(float(row_copy["euro"]) / 2, 2)
            row_copy["euro"] = str(halved)
        tables_insert_data["deal"]["data"].append(row_copy)
        
    reader = csv.DictReader(open(DEAL_SPRINGER_OPT_OUT_FILE, "r"))
    print(colorise("Processing Springer Opt-Out file...", "green"))
    for row in reader:
        row_copy = deepcopy(row) # work on a deep copy since we make some DEAL-specific changes 
        row_copy["opt_out"] = "TRUE"
        if row_copy["publisher"] in DEAL_IMPRINTS["Springer Nature"]:
                row_copy["publisher"] = "Springer Nature"
        institution = row_copy["institution"]
        try:
            row_copy["country"] = transformative_agreements_institution_countries[institution]
        except KeyError:
            if institution not in institution_key_errors:
                institution_key_errors.append(institution)
        tables_insert_data["deal"]["data"].append(row_copy)

    reader = csv.DictReader(open(transformative_agreements_file_name, "r"))
    print(colorise("Processing Transformative Agreements file...", "green"))
    for row in reader:
        if reader.line_num % 10000 == 0:
            print(str(reader.line_num) + " records processed")
        institution = row["institution"]
        publisher = row["publisher"]
        issn = row["issn"]
        doi = row["doi"]
        # colons cannot be escaped in URL queries to the cubes server, so we have
        # to remove them here
        row["journal_full_title"] = row["journal_full_title"].replace(":", "")
        title = row["journal_full_title"]
        try:
            row["country"] = transformative_agreements_institution_countries[institution]
        except KeyError:
            if institution not in institution_key_errors:
                institution_key_errors.append(institution)
        tables_insert_data["transformative_agreements"]["data"].append(row)
        if row["euro"] != "NA":
            tables_insert_data["combined"]["data"].append(row)
        if row["agreement"] == "DEAL Wiley Germany":
            # DEAL Wiley
            row_copy = deepcopy(row)
            row_copy["opt_out"] = "FALSE"
            if row_copy["period"] == "2019":
                # Special rule: Half 2019 costs since DEAL only started in 07/19 
                halved = round(float(row["euro"]) / 2, 2)
                row_copy["euro"] = str(halved)
            if row_copy["publisher"] in DEAL_IMPRINTS["Wiley-Blackwell"]:
                row_copy["publisher"] = "Wiley-Blackwell"
            tables_insert_data["deal"]["data"].append(row_copy)
            
        if row["agreement"] == "DEAL Springer Nature Germany":
            row_copy = deepcopy(row)
            # DEAL SN
            row_copy["opt_out"] = "FALSE"
            if row_copy["publisher"] in DEAL_IMPRINTS["Springer Nature"]:
                row_copy["publisher"] = "Springer Nature"
            tables_insert_data["deal"]["data"].append(row_copy)
            
        if publisher != "Springer Nature":
            continue

        journal_id = scc._get_springer_journal_id_from_doi(doi, issn)
        journal_id_title_map[journal_id] = title
        try:
            pub_year = article_pubyears[journal_id][doi]
        except KeyError:
            pub_year = row["period"]

        if journal_id not in summarised_transformative_agreements:
            summarised_transformative_agreements[journal_id] = {}
        if pub_year not in summarised_transformative_agreements[journal_id]:
            summarised_transformative_agreements[journal_id][pub_year] = 1
        else:
            summarised_transformative_agreements[journal_id][pub_year] += 1
    if institution_key_errors:
        print("KeyError: The following institutions were not found in the " +
              "institutions_transformative_agreements file:")
        for institution in institution_key_errors:
            print(institution)
        sys.exit()
    print(colorise("Generating Springer Compact Coverage data...", "green"))
    
    for journal_id, info in journal_coverage.items():
        for year, stats in info["years"].items():
            row = {
                "publisher": "Springer Nature",
                "journal_full_title": info["title"],
                "period": year,
                "is_hybrid": "TRUE",
                "num_journal_total_articles": stats["num_journal_total_articles"],
                "num_journal_oa_articles": stats["num_journal_oa_articles"]
            }
            try:
                row["num_springer_compact_articles"] = summarised_transformative_agreements[journal_id][year]
            except KeyError:
                row["num_springer_compact_articles"] = 0
            tables_insert_data["springer_compact_coverage"]["data"].append(row)

    institution_countries = {}
    institution_ror_ids = {}

    reader = csv.DictReader(open(INSTITUTIONS_FILE, "r"))
    for row in reader:
        cubes_name = row["institution_cubes_name"]
        institution_name = row["institution"]
        country = row["country"]
        ror_id = 'NA'
        if row["ror_id"].startswith("https://ror.org/"):
            ror_id = row["ror_id"][16:] # Remove 'https://ror.org/'
        institution_countries[institution_name] = country
        institution_ror_ids[institution_name] = ror_id
        if institution_name not in tables_insert_data:
            tables_insert_data[institution_name] = {
                "fields": apc_fields,
                "cubes_name": cubes_name,
                "data": []
            }

    print(colorise("Processing APC file...", "green"))
    reader = csv.DictReader(open(apc_file_name, "r"))
    for row in reader:
        if reader.line_num % 10000 == 0:
            print(str(reader.line_num) + " records processed")
        institution = row["institution"]
        # colons cannot be escaped in URL queries to the cubes server, so we have
        # to remove them here
        row["journal_full_title"] = row["journal_full_title"].replace(":", "")
        row["country"] = institution_countries[institution]
        row["institution_ror"] = institution_ror_ids[institution]
        tables_insert_data[institution]["data"].append(row)
        tables_insert_data["openapc"]["data"].append(row)
        tables_insert_data["combined"]["data"].append(row)
        # DEAL Wiley
        if row["publisher"] in DEAL_IMPRINTS["Wiley-Blackwell"] and row["country"] == "DEU" and row["is_hybrid"] == "FALSE":
            if row["period"] in ["2019", "2020", "2021", "2022"]:
                row_copy = deepcopy(row)
                row_copy["publisher"] = "Wiley-Blackwell" # Imprint normalization
                row_copy["opt_out"] = "FALSE"
                tables_insert_data["deal"]["data"].append(row_copy)
        # DEAL Springer
        if row["publisher"] in DEAL_IMPRINTS["Springer Nature"] and row["country"] == "DEU" and row["is_hybrid"] == "FALSE":
            if row_copy["period"] in ["2020", "2021", "2022"]:
                row_copy = deepcopy(row)
                row_copy["opt_out"] = "FALSE"
                row_copy["publisher"] = "Springer Nature"
                tables_insert_data["deal"]["data"].append(row_copy)

    print(colorise("Populating database tables...", "green"))
    for table_name, data in tables_insert_data.items():
        print("Table '" + data["cubes_name"] + "'...")
        table = sqlalchemy.Table(data["cubes_name"], metadata, autoload=False, schema=schema)
        if table.exists():
            table.drop(checkfirst=False)
        init_table(table, data["fields"])
        connectable.execute(table.insert(), data["data"])

def generate_model_file(path):
    content = ""
    with open("static/templates/MODEL_FIRST_PART", "r") as model:
        content += model.read()

    with open("static/templates/MODEL_CUBE_STATIC_PART", "r") as model:
        static_part = model.read()

    reader = csv.DictReader(open(INSTITUTIONS_FILE, "r"))
    for row in reader:
        content += "        ,\n        {\n"
        content += '            "name": "{}",\n'.format((row["institution_cubes_name"]))
        content += '            "label": "{} openAPC data cube",\n'.format((row["institution_full_name"]))
        content += static_part

    with open("static/templates/MODEL_LAST_PART", "r") as model:
        content += model.read()

    output_file = os.path.join(path, "model.json")
    with open(output_file, "w") as model:
        model.write(content)

def generate_yamls(path):
    with open("static/templates/YAML_STATIC_PART", "r") as yaml:
        yaml_static = yaml.read()

    reader = csv.DictReader(open(INSTITUTIONS_FILE, "r"))
    for row in reader:
        content = "name: " + row["institution_full_name"] + u"\n"
        content += "slug: " + row["institution_cubes_name"] + u"\n"
        content += "tagline: " + row["institution_full_name"] + u" APC data\n"
        content += "source: Open APC\n"
        content += "source_url: https://github.com/OpenAPC/openapc-de\n"
        content += "data_url: https://github.com/OpenAPC/openapc-de/blob/master/data/apc_de.csv\n"
        content += "continent: " + row["continent"] + u"\n"
        content += "country: " + row["country"] + u"\n"
        content += "state: " + row["state"] + u"\n"
        content += "level: kommune\n"
        content += "dataset: '" + row["institution_cubes_name"] + "'\n"
        content += yaml_static

        out_file_name = row["institution_cubes_name"] + ".yaml"
        out_file_path = os.path.join(path, out_file_name)
        with open(out_file_path, "w") as outfile:
            outfile.write(content)


if __name__ == '__main__':
    main()
