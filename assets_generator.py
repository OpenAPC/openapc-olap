#!/usr/bin/env python3
# -*- coding: UTF-8 -*-

import argparse
import csv
import configparser
import json
import os
import sys

from util import colorise
import offsetting_coverage as oc

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

APC_DE_FILE = "apc_de.csv"
OFFSETTING_FILE = "offsetting.csv"
OPENAIRE_FILE = "OpenAIRE_APC_DE_20190131.csv"
SIMULATED_OFFSETTING_FILE_NON_OA = "simulated_data/offsetting_germany/springer_pub_non_oa_enriched.csv"
SIMULATED_OFFSETTING_FILE_OA = "simulated_data/offsetting_germany/springer_pub_oa_enriched.csv"

def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("job", choices=["tables", "model", "yamls", "db_settings", "coverage_stats", "simulated_coverage_stats"])
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
        oc.update_coverage_stats([OFFSETTING_FILE], args.num_api_lookups, args.refetch)
    elif args.job == "simulated_coverage_stats":
        oc.update_coverage_stats([SIMULATED_OFFSETTING_FILE_NON_OA, SIMULATED_OFFSETTING_FILE_OA], args.num_api_lookups, args.refetch)
        
        
        
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

    offsetting_fields = [
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
    ]

    offsetting_coverage_fields = [
        ("period", "string"),
        ("publisher", "string"),
        ("journal_full_title", "string"),
        ("is_hybrid", "string"),
        ("num_offsetting_articles", "float"),
        ("num_journal_total_articles", "float"),
        ("num_journal_oa_articles", "float")
    ]

    metadata = sqlalchemy.MetaData(bind=connectable)

    openapc_table = sqlalchemy.Table("openapc", metadata, autoload=False, schema=schema)
    if openapc_table.exists():
        openapc_table.drop(checkfirst=False)
    init_table(openapc_table, apc_fields)
    openapc_insert_command = openapc_table.insert()
    
    openaire_table = sqlalchemy.Table("openaire", metadata, autoload=False, schema=schema)
    if openaire_table.exists():
        openaire_table.drop(checkfirst=False)
    init_table(openaire_table, apc_fields)
    openaire_insert_command = openaire_table.insert()
    
    offsetting_table = sqlalchemy.Table("offsetting", metadata, autoload=False, schema=schema)
    if offsetting_table.exists():
        offsetting_table.drop(checkfirst=False)
    init_table(offsetting_table, offsetting_fields)
    offsetting_insert_command = offsetting_table.insert()
    
    simulated_offsetting_table = sqlalchemy.Table("simulated_offsetting", metadata, autoload=False, schema=schema)
    if simulated_offsetting_table.exists():
        simulated_offsetting_table.drop(checkfirst=False)
    init_table(simulated_offsetting_table, offsetting_fields)
    simulated_offsetting_insert_command = simulated_offsetting_table.insert()
    
    combined_table = sqlalchemy.Table("combined", metadata, autoload=False, schema=schema)
    if combined_table.exists():
        combined_table.drop(checkfirst=False)
    init_table(combined_table, apc_fields)
    combined_insert_command = combined_table.insert()
    
    offsetting_coverage_table = sqlalchemy.Table("offsetting_coverage", metadata, autoload=False, schema=schema)
    if offsetting_coverage_table.exists():
        offsetting_coverage_table.drop(checkfirst=False)
    init_table(offsetting_coverage_table, offsetting_coverage_fields)
    offsetting_coverage_insert_command = offsetting_coverage_table.insert()
    
    simulated_offsetting_coverage_table = sqlalchemy.Table("simulated_offsetting_coverage", metadata, autoload=False, schema=schema)
    if simulated_offsetting_coverage_table.exists():
        simulated_offsetting_coverage_table.drop(checkfirst=False)
    init_table(simulated_offsetting_coverage_table, offsetting_coverage_fields)
    simulated_offsetting_coverage_insert_command = simulated_offsetting_coverage_table.insert()
    
    # a dict to store individual insert commands for every table
    tables_insert_commands = {
        "openapc": openapc_insert_command,
        "openaire": openaire_insert_command,
        "offsetting": offsetting_insert_command,
        "simulated_offsetting": simulated_offsetting_insert_command,
        "combined": combined_insert_command,
        "offsetting_coverage": offsetting_coverage_insert_command,
        "simulated_offsetting_coverage": simulated_offsetting_coverage_insert_command
    }

    offsetting_institution_countries = {}

    reader = csv.DictReader(open("static/institutions_offsetting.csv", "r"))
    for row in reader:
        institution_name = row["institution"]
        country = row["country"]
        offsetting_institution_countries[institution_name] = country

    journal_coverage = None
    article_pubyears = None
    try:
        cache_file = open(oc.COVERAGE_CACHE_FILE, "r")
        journal_coverage = json.loads(cache_file.read())
        cache_file.close()
        cache_file = open(oc.PUBDATES_CACHE_FILE, "r")
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
        

    summarised_offsetting = {}
    summarised_simulated_offsetting = {}
    
    journal_id_title_map = {}
    
    for file_name in [OFFSETTING_FILE, SIMULATED_OFFSETTING_FILE_NON_OA, SIMULATED_OFFSETTING_FILE_OA]:
        reader = csv.DictReader(open(file_name, "r"))
        institution_key_errors = []
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
                row["country"] = offsetting_institution_countries[institution]
            except KeyError as ke:
                if institution not in institution_key_errors:
                    institution_key_errors.append(institution)
            if file_name == OFFSETTING_FILE:
                tables_insert_commands["offsetting"].execute(row)
                if row["euro"] != "NA":
                    tables_insert_commands["combined"].execute(row)
            tables_insert_commands["simulated_offsetting"].execute(row)
            
            if publisher != "Springer Nature":
                continue
            
            journal_id = oc._get_springer_journal_id_from_doi(doi, issn)
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
                
            if journal_id not in summarised_simulated_offsetting:
                summarised_simulated_offsetting[journal_id] = {}
            if pub_year not in summarised_simulated_offsetting[journal_id]:
                summarised_simulated_offsetting[journal_id][pub_year] = {"num_offsetting": 1, "additional_oa": 0}
            else:
                summarised_simulated_offsetting[journal_id][pub_year]["num_offsetting"] += 1
            if file_name == SIMULATED_OFFSETTING_FILE_NON_OA:
                # articles from SIMULATED_OFFSETTING_FILE_OA are already OA by now, so they would not increase the simulated OA count.
                summarised_simulated_offsetting[journal_id][pub_year]["additional_oa"] += 1
            
            
            if file_name == OFFSETTING_FILE:
                if journal_id not in summarised_offsetting:
                    summarised_offsetting[journal_id] = {}
                if pub_year not in summarised_offsetting[journal_id]:
                    summarised_offsetting[journal_id][pub_year] = 1
                else:
                    summarised_offsetting[journal_id][pub_year] += 1
    if institution_key_errors:
        print("KeyError: The following institutions were not found in the " +
              "institutions_offsetting file:")
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
                    row["num_offsetting_articles"] = summarised_offsetting[journal_id][year]
                except KeyError:
                    row["num_offsetting_articles"] = 0
                tables_insert_commands["offsetting_coverage"].execute(row)
                try:
                    row["num_offsetting_articles"] = summarised_simulated_offsetting[journal_id][year]["num_offsetting"]
                except KeyError:
                    row["num_offsetting_articles"] = 0
                try:
                    row["num_journal_oa_articles"] += summarised_simulated_offsetting[journal_id][year]["additional_oa"]
                except KeyError:
                    pass
                tables_insert_commands["simulated_offsetting_coverage"].execute(row)
    
    institution_countries = {}

    reader = csv.DictReader(open("static/institutions.csv", "r"))
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
    
    reader = csv.DictReader(open(APC_DE_FILE, "r"))
    for row in reader:
        institution = row["institution"]
        # colons cannot be escaped in URL queries to the cubes server, so we have
        # to remove them here
        row["journal_full_title"] = row["journal_full_title"].replace(":", "")
        row["country"] = institution_countries[institution]
        tables_insert_commands[institution].execute(row)
        tables_insert_commands["openapc"].execute(row)
        tables_insert_commands["combined"].execute(row)
        
    reader = csv.DictReader(open(OPENAIRE_FILE, "r"))
    for row in reader:
        institution = row["institution"]
        # colons cannot be escaped in URL queries to the cubes server, so we have
        # to remove them here
        row["journal_full_title"] = row["journal_full_title"].replace(":", "")
        row["country"] = institution_countries[institution]
        tables_insert_commands["openaire"].execute(row)

def generate_model_file(path):
    content = ""
    with open("static/templates/MODEL_FIRST_PART", "r") as model:
        content += model.read()

    with open("static/templates/MODEL_CUBE_STATIC_PART", "r") as model:
        static_part = model.read()

    reader = csv.DictReader(open("static/institutions.csv", "r"))
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

    reader = csv.DictReader(open("static/institutions.csv", "r"))
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
