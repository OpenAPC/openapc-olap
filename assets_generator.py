#!/usr/bin/env python3
# -*- coding: UTF-8 -*-

import argparse
import csv
import configparser
from copy import deepcopy
from datetime import datetime
import json
import os
import re
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
ADDITIONAL_COSTS_FILE = "../openapc-de/data/apc_de_additional_costs.csv"

CUBES_LIST_FILE = "institutional_cubes.csv"
CUBES_PRIORITIES = ["apc", "apc_ac", "bpc", "ta", "deal"] # Treemap hierarchy menu order from left to right

DEAL_WILEY_START_YEAR = datetime(2019, 1, 1)
DEAL_SPRINGER_START_YEAR = datetime(2020, 1, 1)

DEAL_IMPRINTS = {
    "Wiley-Blackwell": ["Wiley-Blackwell", "EMBO", "American Geophysical Union (AGU)", "International Union of Crystallography (IUCr)", "The Econometric Society"],
    "Springer Nature": ["Springer Nature", "Zhejiang University Press"]
}

URL_WITHOUT_SCHEME_RE = re.compile(r"^http(s)?:\/\/(?P<path>.*?)$")

MODEL_STATIC_FILES = {
    "apc": "MODEL_CUBE_STATIC_PART",
    "apc_ac": "MODEL_CUBE_STATIC_PART_AC",
    "bpc": "MODEL_CUBE_STATIC_PART_BPC",
    "ta": "MODEL_CUBE_STATIC_PART_TA",
    "deal": "MODEL_CUBE_STATIC_PART_DEAL"
}

YAML_STATIC_FILES = {
    "apc": "YAML_STATIC_PART_APC",
    "apc_ac": "YAML_STATIC_PART_APC_AC",
    "bpc": "YAML_STATIC_PART_BPC",
    "ta": "YAML_STATIC_PART_TA",
    "deal": "YAML_STATIC_PART_DEAL"
}

TABLE_SCHEMAS = {
    "bpc": [
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
    ],
    "apc": [
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
    ],
    "apc_ac": [
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
        ("institution_ror", "string"),
        ("cost_type", "string"),
        ("cost_category", "string"),
        ("publication_key", "string")
    ],
    "deal": [
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
        ("institution_ror", "string"),
        ("opt_out", "string")
    ],
    "ta": [
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
        create_cubes_tables(engine)
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

def create_cubes_tables(connectable, schema="openapc_schema"):

    springer_compact_coverage_fields = [
        ("period", "string"),
        ("publisher", "string"),
        ("journal_full_title", "string"),
        ("is_hybrid", "string"),
        ("num_springer_compact_articles", "float"),
        ("num_journal_total_articles", "float"),
        ("num_journal_oa_articles", "float")
    ]
    
    doi_lookup_fields = [
        ("institution", "string"),
        ("institution_ror", "string"),
        ("institution_full_name", "string"),
        ("euro", "string"),
        ("period", "string"),
        ("doi", "string"),
        ("url", "string")
    ]

    metadata = sqlalchemy.MetaData(bind=connectable)

    # a dict to store individual insert commands and data for static tables
    static_tables_data = {
        "doi_lookup": {
            "fields": doi_lookup_fields,
            "cubes_name": "doi_lookup",
            "data": []
        },
        "openapc": {
            "fields": TABLE_SCHEMAS["apc"],
            "cubes_name": "openapc",
            "data": []
        },
        "openapc_ac": {
            "fields": TABLE_SCHEMAS["apc_ac"],
            "cubes_name": "openapc_ac",
            "data": []
        },
        "transformative_agreements": {
            "fields": TABLE_SCHEMAS["ta"],
            "cubes_name": "transformative_agreements",
            "data": []
        },
        "bpc": {
            "fields": TABLE_SCHEMAS["bpc"],
            "cubes_name": "bpc",
            "data": []
        },
        "combined": {
            "fields": TABLE_SCHEMAS["apc"],
            "cubes_name": "combined",
            "data": []
        },
        "springer_compact_coverage": {
             "fields": springer_compact_coverage_fields,
             "cubes_name": "springer_compact_coverage",
             "data": []
        },
        "deal": {
            "fields": TABLE_SCHEMAS["deal"],
            "cubes_name": "deal",
            "data": []
        }
    }

    # a dict to store individual insert commands and data for institutional tables
    institutional_tables_data = {}

    additional_cost_data = {}

    print(colorise("Processing additional costs file...", "green"))
    reader = csv.DictReader(open(ADDITIONAL_COSTS_FILE, "r"))
    for row in reader:
        cost_dict = {}
        doi = None
        for column, value in row.items():
            if column == "doi":
                doi = value
            else:
                try:
                    value = float(value)
                    cost_dict[column] = value
                except ValueError:
                    pass
        if cost_dict:
            additional_cost_data[doi] = cost_dict

    institution_lookup_table = _create_institution_lookup_table()

    print(colorise("Processing BPC file...", "green"))
    reader = csv.DictReader(open(BPC_FILE, "r"))
    bpc_data = []
    for row in reader:
        row["book_title"] = row["book_title"].replace(":", "")
        institution = row["institution"]
        _insert_into_institutional_tables_data(institutional_tables_data, institution_lookup_table, "bpc", row)
        row["country"] = institution_lookup_table[institution]["country"]
        static_tables_data["bpc"]["data"].append(row)
        ror_id = institution_lookup_table[institution]["ror_id"]
        full_name = institution_lookup_table[institution]["full_name"]
        lookup_data = _create_lookup_data(row, ror_id, full_name, "bpc")
        if lookup_data:
            static_tables_data["doi_lookup"]["data"].append(lookup_data)

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
            row_copy["country"] = institution_lookup_table[institution]["country"]
        except KeyError:
            if institution not in institution_key_errors:
                institution_key_errors.append(institution)
        if row_copy["period"] == "2019":
            # Special rule: Half 2019 costs since DEAL only started in 07/19
            halved = round(float(row_copy["euro"]) / 2, 2)
            row_copy["euro"] = str(halved)
        static_tables_data["deal"]["data"].append(row_copy)
        _insert_into_institutional_tables_data(institutional_tables_data, institution_lookup_table, "deal", row_copy)
        institution_lookup_table[institution]["deal_participant"] = True
        
    reader = csv.DictReader(open(DEAL_SPRINGER_OPT_OUT_FILE, "r"))
    print(colorise("Processing Springer Opt-Out file...", "green"))
    for row in reader:
        row_copy = deepcopy(row) # work on a deep copy since we make some DEAL-specific changes 
        row_copy["opt_out"] = "TRUE"
        if row_copy["publisher"] in DEAL_IMPRINTS["Springer Nature"]:
                row_copy["publisher"] = "Springer Nature"
        institution = row_copy["institution"]
        try:
            row_copy["country"] = institution_lookup_table[institution]["country"]
        except KeyError:
            if institution not in institution_key_errors:
                institution_key_errors.append(institution)
        static_tables_data["deal"]["data"].append(row_copy)
        _insert_into_institutional_tables_data(institutional_tables_data, institution_lookup_table, "deal", row_copy)
        institution_lookup_table[institution]["deal_participant"] = True

    reader = csv.DictReader(open(TRANSFORMATIVE_AGREEMENTS_FILE, "r"))
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
            row["country"] = institution_lookup_table[institution]["country"]
        except KeyError:
            if institution not in institution_key_errors:
                institution_key_errors.append(institution)
        static_tables_data["transformative_agreements"]["data"].append(row)
        _insert_into_institutional_tables_data(institutional_tables_data, institution_lookup_table, "ta", row)
        ror_id = institution_lookup_table[institution]["ror_id"]
        full_name = institution_lookup_table[institution]["full_name"]
        lookup_data = _create_lookup_data(row, ror_id, full_name, "transformative_agreements")
        if lookup_data:
            static_tables_data["doi_lookup"]["data"].append(lookup_data)
        if row["euro"] != "NA":
            static_tables_data["combined"]["data"].append(row)
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
            static_tables_data["deal"]["data"].append(row_copy)
            _insert_into_institutional_tables_data(institutional_tables_data, institution_lookup_table, "deal", row_copy)
            institution_lookup_table[institution]["deal_participant"] = True

        if row["agreement"] == "DEAL Springer Nature Germany":
            row_copy = deepcopy(row)
            # DEAL SN
            row_copy["opt_out"] = "FALSE"
            if row_copy["publisher"] in DEAL_IMPRINTS["Springer Nature"]:
                row_copy["publisher"] = "Springer Nature"
            static_tables_data["deal"]["data"].append(row_copy)
            _insert_into_institutional_tables_data(institutional_tables_data, institution_lookup_table, "deal", row_copy)
            institution_lookup_table[institution]["deal_participant"] = True

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
            static_tables_data["springer_compact_coverage"]["data"].append(row)

    print(colorise("Processing APC file...", "green"))
    reader = csv.DictReader(open(APC_DE_FILE, "r"))
    for row in reader:
        if reader.line_num % 10000 == 0:
            print(str(reader.line_num) + " records processed")
        institution = row["institution"]
        doi = row["doi"]
        # colons cannot be escaped in URL queries to the cubes server, so we have
        # to remove them here
        row["journal_full_title"] = row["journal_full_title"].replace(":", "")
        row["country"] = institution_lookup_table[institution]["country"]
        ror_id = institution_lookup_table[institution]["ror_id"]
        full_name = institution_lookup_table[institution]["full_name"]
        row["institution_ror"] = ror_id
        static_tables_data["openapc"]["data"].append(row)
        lookup_data = _create_lookup_data(row, ror_id, full_name, "openapc")
        if lookup_data:
            static_tables_data["doi_lookup"]["data"].append(lookup_data)
        static_tables_data["combined"]["data"].append(row)
        _insert_into_institutional_tables_data(institutional_tables_data, institution_lookup_table, "apc", row)
        # create copy with ac fields
        row_copy = deepcopy(row)
        row_copy["publication_key"] = _create_publication_key(row)
        row_copy["cost_type"] = "apc"
        row_copy["cost_category"] = "APC"
        static_tables_data["openapc_ac"]["data"].append(row_copy)
        _insert_into_institutional_tables_data(institutional_tables_data, institution_lookup_table, "apc_ac", row_copy)
        if doi in additional_cost_data:
            for cost_type, value in additional_cost_data[doi].items():
                row_copy = deepcopy(row)
                row_copy["cost_type"] = cost_type
                row_copy["cost_category"] = "Additional Cost"
                row_copy["euro"] = value
                row_copy["publication_key"] = _create_publication_key(row)
                _insert_into_institutional_tables_data(institutional_tables_data, institution_lookup_table, "apc_ac", row_copy)
                static_tables_data["openapc_ac"]["data"].append(row_copy)
        # DEAL Wiley
        if row["publisher"] in DEAL_IMPRINTS["Wiley-Blackwell"] and row["country"] == "DEU" and row["is_hybrid"] == "FALSE":
            if datetime.strptime(row["period"], "%Y") > DEAL_WILEY_START_YEAR:
                row_copy = deepcopy(row)
                row_copy["publisher"] = "Wiley-Blackwell" # Imprint normalization
                row_copy["opt_out"] = "FALSE"
                static_tables_data["deal"]["data"].append(row_copy)
                _insert_into_institutional_tables_data(institutional_tables_data, institution_lookup_table, "deal", row_copy)
        # DEAL Springer
        if row["publisher"] in DEAL_IMPRINTS["Springer Nature"] and row["country"] == "DEU" and row["is_hybrid"] == "FALSE":
            if datetime.strptime(row["period"], "%Y") > DEAL_SPRINGER_START_YEAR:
                row_copy = deepcopy(row)
                row_copy["opt_out"] = "FALSE"
                row_copy["publisher"] = "Springer Nature"
                static_tables_data["deal"]["data"].append(row_copy)
                _insert_into_institutional_tables_data(institutional_tables_data, institution_lookup_table, "deal", row_copy)

    _postprocess_institutional_tables(institutional_tables_data, institution_lookup_table)
    _report_non_apc_cubes(institutional_tables_data)
    print(colorise("Populating database tables...", "green"))
    for table_name, data in static_tables_data.items():
        print("Aggregated table '" + data["cubes_name"] + "'...")
        table = sqlalchemy.Table(data["cubes_name"], metadata, autoload=False, schema=schema)
        if table.exists():
            table.drop(checkfirst=False)
        init_table(table, data["fields"])
        connectable.execute(table.insert(), data["data"])
    with open(CUBES_LIST_FILE, "w") as cubes_list:
        writer = csv.writer(cubes_list)
        writer.writerow(["institution", "cube_name", "full_name", "cube_type", "priority"])
        for institution, institutional_data in institutional_tables_data.items():
            for table_type, data in institutional_data.items():
                print("Institutional " + table_type + " table '" + data["cubes_name"] + "'...")
                table = sqlalchemy.Table(data["cubes_name"], metadata, autoload=False, schema=schema)
                if table.exists():
                    table.drop(checkfirst=False)
                init_table(table, data["fields"])
                connectable.execute(table.insert(), data["data"])
                writer.writerow([institution, data["cubes_name"], data["full_name"], table_type, data["priority"]])

def _is_cubes_institution(institutions_row):
    cubes_name = institutions_row["institution_cubes_name"]
    if cubes_name and cubes_name != "NA":
        return True
    return False

def _create_lookup_data(row, ror_id, full_name, cube_name):
    facts_doi_url = "https://olap.openapc.net/cube/{}/facts?cut=doi:{}"
    data = {}
    if row["doi"] == "NA":
        return {}
    for key in ["institution", "euro", "period", "doi"]:
        data[key] = row[key]
    data["institution_ror"] = ror_id
    data["institution_full_name"] = full_name
    data["url"] = facts_doi_url.format(cube_name, row["doi"])
    return data

def _create_publication_key(row):
    if row["doi"] and row["doi"] != 'NA':
        return row["doi"]
    if row["url"] and row["url"] != 'NA':
        match = URL_WITHOUT_SCHEME_RE.match(row["url"])
        if match:
            return match.group("path")
        else:
            return row["url"]
    raise Exception("Error while processing row " + ",".join(row) + ": Cound not extract a publication key!")

def generate_model_file(path):
    if not os.path.isfile(CUBES_LIST_FILE):
        print('Error: Cubes list file ("' + CUBES_LIST_FILE + '") not found. ' +
              'Run this script with the "tables" job first to generate it.')
        sys.exit()
    content = ""
    with open("static/templates/MODEL_FIRST_PART", "r") as model:
        content += model.read()

    model_contents = {}
    for model_type, file_name in MODEL_STATIC_FILES.items():
        with open("static/templates/" + file_name, "r") as model:
            model_contents[model_type] = model.read()

    reader = csv.DictReader(open(CUBES_LIST_FILE, "r"))
    for row in reader:
        content += "        ,\n        {\n"
        content += '            "name": "{}",\n'.format((row["cube_name"]))
        content += '            "label": "{} openAPC data cube",\n'.format((row["full_name"]))
        content += model_contents[row["cube_type"]]

    with open("static/templates/MODEL_LAST_PART", "r") as model:
        content += model.read()

    output_file = os.path.join(path, "model.json")
    with open(output_file, "w") as model:
        model.write(content)

# - Remove institutional ac tables if no additional costs are present
# - Remove institutional deal tables if no TA entries with a deal agreemnt 
def _postprocess_institutional_tables(institutional_tables_data, institution_lookup_table):
    deal_deleted = []
    for institution, data in deepcopy(institutional_tables_data).items():
        if "apc_ac" in data:
            for row in data["apc_ac"]["data"]:
                if row["cost_type"] != "apc":
                    break
            else:
                del institutional_tables_data[institution]["apc_ac"]
        if "deal" in data:
            if not institution_lookup_table[institution].get("deal_participant", False):
                deal_deleted.append(institution)
                del institutional_tables_data[institution]["deal"]
    msg = ("A deal cube will not be generated for these {} institutions " +
           "since they did not report hybrid DEAL TA data: {}\n")
    msg = msg.format(len(deal_deleted), ", ".join(deal_deleted))
    print(colorise(msg, "yellow"))

def _report_non_apc_cubes(institutional_tables_data):
    non_apc_cubes = {}
    for institution, cubes in institutional_tables_data.items():
        for cube_type, data in cubes.items():
            if cube_type == "apc":
                continue
            if cube_type not in non_apc_cubes:
                non_apc_cubes[cube_type] = []
            non_apc_cubes[cube_type].append(institution)
    for cube_type, institution_list in non_apc_cubes.items():
        msg = "Additional {} cubes will be generated for {} institutions: {}\n"
        msg = msg.format(cube_type, len(institution_list), ", ".join(sorted(institution_list)))
        print(colorise(msg, "cyan"))

def _insert_into_institutional_tables_data(institutional_tables_data, institution_lookup_table, table_type, row):
    institution = row["institution"]
    full_name = institution_lookup_table[institution]["full_name"]
    cube_name = institution_lookup_table[institution]["cube_name"]
    if not cube_name or cube_name == "NA":
        return
    if institution not in institutional_tables_data:
        institutional_tables_data[institution] = {}
    target_cube_name = cube_name
    if table_type != "apc":
        target_cube_name += "_" + table_type
    if table_type not in institutional_tables_data[institution]:
        institutional_tables_data[institution][table_type] = {
            "fields": TABLE_SCHEMAS[table_type],
            "cubes_name": target_cube_name,
            "full_name": full_name,
            "data": []
        }
    institutional_tables_data[institution][table_type]["data"].append(deepcopy(row))
    # create/reorder priority
    priority = 0
    for priority_type in CUBES_PRIORITIES:
        if priority_type in institutional_tables_data[institution]:
            institutional_tables_data[institution][priority_type]["priority"] = priority
            priority += 1

def _create_institution_lookup_table():
    print(colorise("Processing institutions file...", "green"))
    reader = csv.DictReader(open(INSTITUTIONS_FILE, "r"))
    ret = {}
    for row in reader:
        institution_name = row["institution"]
        ror_id = 'NA'
        if row["ror_id"].startswith("https://ror.org/"):
            ror_id = row["ror_id"][16:] # Remove 'https://ror.org/'
        ret[institution_name] = {
            "continent": row["continent"],
            "country": row["country"],
            "state": row["state"],
            "ror_id": ror_id,
            "full_name": row["institution_full_name"],
            "cube_name": row["institution_cubes_name"]
        }
    return ret

def _get_additional_costs_institutions():
    additional_costs_institutions = []
    additional_costs_dois = []
    reader = csv.DictReader(open(ADDITIONAL_COSTS_FILE, "r"))
    for row in reader:
        additional_costs_dois.append(row["doi"])
    reader = csv.DictReader(open(APC_DE_FILE, "r"))
    for row in reader:
        if row["institution"] in additional_costs_institutions:
            continue
        if row["doi"] in additional_costs_dois:
            additional_costs_institutions.append(row["institution"])
    print("The following institutions have additional costs attached: " + ", ".join(additional_costs_institutions))
    return additional_costs_institutions

def generate_yamls(path):
    if not os.path.isfile(CUBES_LIST_FILE):
        print('Error: Cubes list file ("' + CUBES_LIST_FILE + '") not found. ' +
              'Run this script with the "tables" job first to generate it.')
        sys.exit()

    institution_cubes = {}
    print(colorise("Processing cubes list file...", "green"))
    reader = csv.DictReader(open(CUBES_LIST_FILE, "r"))
    for line in reader:
        institution = line["institution"]
        if institution not in institution_cubes:
            institution_cubes[institution] = []
        institution_cubes[institution].append(line)

    yaml_static_contents = {}
    print(colorise("Processing yaml templates...", "green"))
    for model_type, file_name in YAML_STATIC_FILES.items():
        with open("static/templates/" + file_name, "r") as yaml:
            yaml_static_contents[model_type] = yaml.read()

    institution_lookup_table = _create_institution_lookup_table()

    for institution_name, row_list in institution_cubes.items():
        row_list = sorted(row_list, key=lambda x: x["priority"], reverse=False)
        default = row_list[0]["cube_type"]

        institution = institution_lookup_table[institution_name]

        content = 'name: "' + institution["full_name"] + '"\n'
        content += "slug: " + institution["cube_name"] + "\n"
        content += 'tagline: "' + institution["full_name"] + ' publication cost data"\n'
        content += "source: Open APC\n"
        content += "source_url: https://github.com/OpenAPC/openapc-de\n"
        content += "data_url: https://github.com/OpenAPC/openapc-de/blob/master/data/apc_de.csv\n"
        content += "continent: " + institution["continent"] + u"\n"
        content += "country: " + institution["country"] + u"\n"
        content += "state: " + institution["state"] + u"\n"
        content += "level: kommune\n"
        content += "dataset: '" + institution["cube_name"] + "'\n"
        content += "default: " + default + "\n\n"
        content += "hierarchies:\n"

        for row in row_list:
            content += "    " + row["cube_type"] + ":\n"
            content += "        cube: " + row["cube_name"] + "\n"
            content += yaml_static_contents[row["cube_type"]]

        out_file_name = institution["cube_name"] + ".yaml"
        out_file_path = os.path.join(path, out_file_name)
        with open(out_file_path, "w") as outfile:
            outfile.write(content)

if __name__ == '__main__':
    main()
