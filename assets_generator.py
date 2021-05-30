#!/usr/bin/env python3
# -*- coding: UTF-8 -*-

import argparse
import csv
import configparser
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
INSTITUTIONS_FILE = "../openapc-de/data/institutions.csv"
INSTITUTIONS_TRANSFORMATIVE_AGREEMENTS_FILE = "../openapc-de/data/institutions_transformative_agreements.csv"
INSTITUTIONS_BPC_FILE = "../openapc-de/data/institutions_bpcs.csv"

WILEY_IMPRINTS = ["Wiley-Blackwell", "EMBO", "American Geophysical Union (AGU)"]

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
        ("country", "string")
    ]

    deal_wiley_fields = apc_fields + [("opt_out", "string")]

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

    openapc_table = sqlalchemy.Table("openapc", metadata, autoload=False, schema=schema)
    if openapc_table.exists():
        openapc_table.drop(checkfirst=False)
    init_table(openapc_table, apc_fields)
    openapc_insert_command = openapc_table.insert()

    transformative_agreements_table = sqlalchemy.Table("transformative_agreements", metadata, autoload=False, schema=schema)
    if transformative_agreements_table.exists():
        transformative_agreements_table.drop(checkfirst=False)
    init_table(transformative_agreements_table, transformative_agreements_fields)
    transformative_agreements_insert_command = transformative_agreements_table.insert()
    
    bpc_table = sqlalchemy.Table("bpc", metadata, autoload=False, schema=schema)
    if bpc_table.exists():
        bpc_table.drop(checkfirst=False)
    init_table(bpc_table, bpc_fields)
    bpc_insert_command = bpc_table.insert()

    combined_table = sqlalchemy.Table("combined", metadata, autoload=False, schema=schema)
    if combined_table.exists():
        combined_table.drop(checkfirst=False)
    init_table(combined_table, apc_fields)
    combined_insert_command = combined_table.insert()

    springer_compact_coverage_table = sqlalchemy.Table("springer_compact_coverage", metadata,
                                                 autoload=False, schema=schema)
    if springer_compact_coverage_table.exists():
        springer_compact_coverage_table.drop(checkfirst=False)
    init_table(springer_compact_coverage_table, springer_compact_coverage_fields)
    springer_compact_coverage_insert_command = springer_compact_coverage_table.insert()

    deal_wiley_table = sqlalchemy.Table("deal_wiley", metadata, autoload=False,
                                        schema=schema)
    if deal_wiley_table.exists():
        deal_wiley_table.drop(checkfirst=False)
    init_table(deal_wiley_table, deal_wiley_fields)
    deal_wiley_insert_command = deal_wiley_table.insert()

    # a dict to store individual insert commands for every table
    tables_insert_commands = {
        "openapc": openapc_insert_command,
        "transformative_agreements": transformative_agreements_insert_command,
        "bpc": bpc_insert_command,
        "combined": combined_insert_command,
        "springer_compact_coverage": springer_compact_coverage_insert_command,
        "deal_wiley": deal_wiley_insert_command
    }
    
    bpcs_institution_countries = {}

    reader = csv.DictReader(open(INSTITUTIONS_BPC_FILE, "r"))
    for row in reader:
        institution_name = row["institution"]
        country = row["country"]
        bpcs_institution_countries[institution_name] = country

    reader = csv.DictReader(open(BPC_FILE, "r"))
    for row in reader:
        row["book_title"] = row["book_title"].replace(":", "")
        institution = row["institution"]
        row["country"] = bpcs_institution_countries[institution]
        tables_insert_commands["bpc"].execute(row)

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
    for row in reader:
        row["opt_out"] = "TRUE"
        institution = row["institution"]
        try:
            row["country"] = transformative_agreements_institution_countries[institution]
        except KeyError:
            if institution not in institution_key_errors:
                institution_key_errors.append(institution)
        if row["period"] == "2019":
            # Special rule: Half 2019 costs since DEAL only started in 07/19
            halved = round(float(row["euro"]) / 2, 2)
            row["euro"] = str(halved)
        tables_insert_commands["deal_wiley"].execute(row)

    reader = csv.DictReader(open(transformative_agreements_file_name, "r"))

    for row in reader:
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
        tables_insert_commands["transformative_agreements"].execute(row)
        if row["euro"] != "NA":
            tables_insert_commands["combined"].execute(row)
        if row["agreement"] == "DEAL Wiley Germany":
            # DEAL Wiley
            row["opt_out"] = "FALSE"
            if row["period"] == "2019":
                # Special rule: Half 2019 costs since DEAL only started in 07/19 
                halved = round(float(row["euro"]) / 2, 2)
                row["euro"] = str(halved)
            tables_insert_commands["deal_wiley"].execute(row)
            
        if publisher != "Springer Nature":
            continue

        journal_id = scc._get_springer_journal_id_from_doi(doi, issn)
        journal_id_title_map[journal_id] = title
        try:
            pub_year = article_pubyears[journal_id][doi]
        except KeyError:
            msg = (u"Publication year entry not found in article cache for {}. " +
                   "You might have to update the article cache with 'python " +
                   "assets_generator.py coverage_stats'. Using the 'period' " +
                   "column for now.")
            print(colorise(msg.format(doi), "yellow"))
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
            tables_insert_commands["springer_compact_coverage"].execute(row)

    institution_countries = {}

    reader = csv.DictReader(open(INSTITUTIONS_FILE, "r"))
    for row in reader:
        cubes_name = row["institution_cubes_name"]
        institution_name = row["institution"]
        country = row["country"]
        institution_countries[institution_name] = country
        if institution_name not in tables_insert_commands:
            table = sqlalchemy.Table(cubes_name, metadata, autoload=False, schema=schema)
            if table.exists():
                table.drop(checkfirst=False)
            init_table(table, apc_fields)
            insert_command = table.insert()
            tables_insert_commands[institution_name] = insert_command

    reader = csv.DictReader(open(apc_file_name, "r"))
    for row in reader:
        institution = row["institution"]
        # colons cannot be escaped in URL queries to the cubes server, so we have
        # to remove them here
        row["journal_full_title"] = row["journal_full_title"].replace(":", "")
        row["country"] = institution_countries[institution]
        tables_insert_commands[institution].execute(row)
        tables_insert_commands["openapc"].execute(row)
        tables_insert_commands["combined"].execute(row)
        #DEAL Wiley
        if row["publisher"] in WILEY_IMPRINTS and row["country"] == "DEU" and row["is_hybrid"] == "FALSE":
            if row["period"] in ["2019", "2020", "2021", "2022"]:
                tables_insert_commands["deal_wiley"].execute(row)

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
