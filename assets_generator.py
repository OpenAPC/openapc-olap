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
CONTRACTS_FILE = "../openapc-de/data/transformative_agreements/contracts.csv"
INSTITUTIONS_FILE = "../openapc-de/data/institutions.csv"
ADDITIONAL_COSTS_FILE = "../openapc-de/data/apc_de_additional_costs.csv"

CUBES_LIST_FILE = "institutional_cubes.csv"
CUBES_PRIORITIES = ["apc", "apc_ac", "bpc", "contracts", "ta_euro", "ta_count"] # Treemap hierarchy menu order from left to right

URL_WITHOUT_SCHEME_RE = re.compile(r"^http(s)?:\/\/(?P<path>.*?)$")

MODEL_STATIC_FILES = {
    "apc": "MODEL_CUBE_STATIC_PART",
    "apc_ac": "MODEL_CUBE_STATIC_PART_AC",
    "bpc": "MODEL_CUBE_STATIC_PART_BPC",
    "ta_euro": "MODEL_CUBE_STATIC_PART_TA_EURO",
    "ta_count": "MODEL_CUBE_STATIC_PART_TA_COUNT",
    "deal": "MODEL_CUBE_STATIC_PART_DEAL",
    "contracts": "MODEL_CUBE_STATIC_PART_CONTRACTS"
}

YAML_STATIC_FILES = {
    "apc": "YAML_STATIC_PART_APC",
    "apc_ac": "YAML_STATIC_PART_APC_AC",
    "bpc": "YAML_STATIC_PART_BPC",
    "ta_count": "YAML_STATIC_PART_TA_COUNT",
    "ta_euro": "YAML_STATIC_PART_TA_EURO",
    "contracts": "YAML_STATIC_PART_CONTRACTS",
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
    "ta_euro": [
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
        ("contract_name", "string"),
        ("opt_out", "string")
    ],
    "ta_count": [
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
        ("contract_name", "string"),
        ("opt_out", "string")
    ],
    "contracts": [
        ("institution", "string"),
        ("period", "string"),
        ("contract_name", "string"),
        ("cost_type", "string"),
        ("euro", "float"),
        ("country", "string"),
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
        ("contract_name", "string"),
        ("opt_out", "string")
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
        "ta_count": {
            "fields": TABLE_SCHEMAS["ta_count"],
            "cubes_name": "ta_count",
            "data": []
        },
        "ta_euro": {
            "fields": TABLE_SCHEMAS["ta_euro"],
            "cubes_name": "ta_euro",
            "data": []
        },
        "bpc": {
            "fields": TABLE_SCHEMAS["bpc"],
            "cubes_name": "bpc",
            "data": []
        },
        "deal": {
            "fields": TABLE_SCHEMAS["deal"],
            "cubes_name": "deal",
            "data": []
        },
        "contracts": {
            "fields": TABLE_SCHEMAS["contracts"],
            "cubes_name": "contracts",
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
    contracts_lookup_table = _create_contracts_lookup_table()

    print(colorise("Processing BPC file...", "green"))
    reader = csv.DictReader(open(BPC_FILE, "r"))
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
            
    print(colorise("Processing Contracts file...", "green"))
    reader = csv.DictReader(open(CONTRACTS_FILE, "r"))
    for row in reader:
        institution = row["institution"]
        euro = row["euro"]
        if euro == "NA":
            continue
        row["country"] = institution_lookup_table[institution]["country"]
        row["period"] = row["period_from"]
        _insert_into_institutional_tables_data(institutional_tables_data, institution_lookup_table, "contracts", row)
        static_tables_data["contracts"]["data"].append(row)

    institution_key_errors = []

    # TA processing steps:
    # 1) Loop over table data, count non-euro articles for each group_id
    # 2) Look up all extracted group_ids and check if contract-level costs exist. If yes, calculate EAPC.
    # 3) Loop over data again, assign calculated EAPCs where applicable

    ta_rows = []
    group_id_dict = {} # Counts group_id articles + stores possible EAPCs
    reader = csv.DictReader(open(TRANSFORMATIVE_AGREEMENTS_FILE, "r"))
    print(colorise("Processing Transformative Agreements file...", "green"))
    for row in reader:
        if reader.line_num % 10000 == 0:
            print(str(reader.line_num) + " records processed")
        ta_rows.append(row)
        group_id = row["group_id"]
        euro = row["euro"]
        if group_id not in group_id_dict:
            group_id_dict[group_id] = {
                "non_euro_articles": 0 # only count articles which do not have costs already assigned!
            }
        if euro == "NA":
            group_id_dict[group_id]["non_euro_articles"] += 1
    print(colorise("Calculating EAPCs...", "green"))
    for group_id, group_data in group_id_dict.items():
        if group_data["non_euro_articles"] == 0:
            continue
        contract_data = contracts_lookup_table[group_id]
        if contract_data["euro"] != "NA":
            eapc = round(contract_data["euro"] / group_data["non_euro_articles"], 2)
            group_data["eapc"] = eapc
    print(colorise("Creating TA/contract data...", "green"))
    for row_num, row in enumerate(ta_rows):
        if row_num % 10000 == 0:
            print(str(row_num) + " records processed")
        institution = row["institution"]
        publisher = row["publisher"]
        issn = row["issn"]
        doi = row["doi"]
        euro = row["euro"]
        group_id = row["group_id"]
        row["contract_name"] = contracts_lookup_table[group_id]["contract_name"]
        # colons cannot be escaped in URL queries to the cubes server, so we have
        # to remove them here
        row["journal_full_title"] = row["journal_full_title"].replace(":", "")
        title = row["journal_full_title"]
        try:
            row["country"] = institution_lookup_table[institution]["country"]
        except KeyError:
            if institution not in institution_key_errors:
                institution_key_errors.append(institution)
        # Assign possible EAPC
        if row["euro"] == "NA":
            group_data = group_id_dict[group_id]
            if "eapc" in group_data:
                row["euro"] = group_data["eapc"]
        static_tables_data["ta_count"]["data"].append(row)
        _insert_into_institutional_tables_data(institutional_tables_data, institution_lookup_table, "ta_count", row)
        if row["euro"] != "NA":
            static_tables_data["ta_euro"]["data"].append(row)
            _insert_into_institutional_tables_data(institutional_tables_data, institution_lookup_table, "ta_euro", row)
        
        ror_id = institution_lookup_table[institution]["ror_id"]
        full_name = institution_lookup_table[institution]["full_name"]
        lookup_data = _create_lookup_data(row, ror_id, full_name, "transformative_agreements")
        if lookup_data:
            static_tables_data["doi_lookup"]["data"].append(lookup_data)

    if institution_key_errors:
        print("KeyError: The following institutions were not found in the " +
              "institutions_transformative_agreements file:")
        for institution in institution_key_errors:
            print(institution)
        sys.exit()

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
def _postprocess_institutional_tables(institutional_tables_data, institution_lookup_table):
    for institution, data in deepcopy(institutional_tables_data).items():
        if "apc_ac" in data:
            for row in data["apc_ac"]["data"]:
                if row["cost_type"] != "apc":
                    break
            else:
                del institutional_tables_data[institution]["apc_ac"]

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
    
def _create_contracts_lookup_table():
    print(colorise("Processing contracts file...", "green"))
    reader = csv.DictReader(open(CONTRACTS_FILE, "r"))
    ret = {}
    for row in reader:
        if row["euro"] != "NA":
            row["euro"] = float(row["euro"])
        group_id = row["group_id"]
        if group_id not in ret:
            ret[group_id] = row
        else:
            ret[group_id]["euro"] += row["euro"]
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
