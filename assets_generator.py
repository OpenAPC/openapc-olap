#!/usr/bin/env python

import argparse
import csv
import ConfigParser
import json
import os
import sys
import time
import urllib2

from util import UnicodeReader, colorise
import offsetting_coverage as oc

import sqlalchemy
        
ARG_HELP_STRINGS = {
    
    "dir": "A path to a directory where the generated output files should be stored. " +
           "If omitted, output will be written to the current directory.",
    "num_api_lookups": "stop execution after n journal lookups to " +
                        "when performing the coverage_stats job. Useful for " +
                        "reducing API loads and saving results from time to time."
}

APC_DE_FILE = "apc_de.csv"
OFFSETTING_FILE = "offsetting.csv"

def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("job", choices=["tables", "model", "yamls", "db_settings", "coverage_stats"])
    parser.add_argument("-d", "--dir", help=ARG_HELP_STRINGS["dir"])
    parser.add_argument("-n", "--num_api_lookups", type=int, help=ARG_HELP_STRINGS["num_api_lookups"])
    args = parser.parse_args()
    
    path = "."
    if args.dir:
        if os.path.isdir(args.dir):
            path = args.dir
        else:
            print "ERROR: '" + args.dir + "' is no valid directory!"
    
    if args.job == "tables":
        if not os.path.isfile("db_settings.ini"):
            print "ERROR: Database Configuration file db_settings.ini not found!"
            sys.exit()
        scp = ConfigParser.SafeConfigParser()
        scp.read("db_settings.ini")
        try:
            db_user = scp.get("postgres_credentials", "user")
            db_pass = scp.get("postgres_credentials", "pass")
        except (ConfigParser.NoSectionError, ConfigParser.NoOptionError) as e:
            print "ERROR: db_settings.ini is malformed ({})".format(e.message)
            sys.exit()
        psql_uri = "postgresql://" + db_user + ":" + db_pass + "@localhost/openapc_db"
        engine = sqlalchemy.create_engine(psql_uri)
        create_cubes_tables(engine, APC_DE_FILE, OFFSETTING_FILE)
        with engine.begin() as connection:
            connection.execute("GRANT SELECT ON ALL TABLES IN SCHEMA openapc_schema TO cubes_user")
        
        
    elif args.job == "model":
        generate_model_file(path)
    elif args.job == "yamls":
        generate_yamls(path)
    elif args.job == "db_settings":
        if os.path.isfile("db_settings.ini"):
            print "ERROR: db_settings.ini already exists"
            sys.exit()
        scp = ConfigParser.SafeConfigParser()
        scp.add_section('postgres_credentials')
        scp.set('postgres_credentials', 'USER', 'table_creator')
        scp.set('postgres_credentials', 'PASS', 'change_me')
        with open('db_settings.ini', 'w') as config_file:
            scp.write(config_file)
    elif args.job == "coverage_stats":
        oc.update_coverage_stats(OFFSETTING_FILE, args.num_api_lookups)
        
        
        
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

def create_cubes_tables(connectable, apc_file_name, offsetting_file_name, schema="openapc_schema"):
    
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
    
    offsetting_table = sqlalchemy.Table("offsetting", metadata, autoload=False, schema=schema)
    if offsetting_table.exists():
        offsetting_table.drop(checkfirst=False)
    init_table(offsetting_table, offsetting_fields)
    offsetting_insert_command = offsetting_table.insert()
    
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
    
    # a dict to store individual insert commands for every table
    tables_insert_commands = {
        "openapc": openapc_insert_command,
        "offsetting": offsetting_insert_command,
        "combined": combined_insert_command,
        "offsetting_coverage": offsetting_coverage_insert_command
    }
    
    offsetting_institution_countries = {}
    
    reader = UnicodeReader(open("static/institutions_offsetting.csv", "rb"))
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
        msg = "Error while trying to cache file: {}"
        print msg.format(ioe)
        sys.exit()
    except ValueError as ve:
        msg = "Error while trying to decode cache structure in: {}"
        print msg.format(ve.message)
        sys.exit()
    
    summarised_offsetting = {}
    issn_title_map = {}
    
    reader = UnicodeReader(open(offsetting_file_name, "rb"))
    institution_key_errors = []
    for row in reader:
        institution = row["institution"]
        publisher = row["publisher"]
        is_hybrid = row["is_hybrid"]
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
        tables_insert_commands["offsetting"].execute(row)
        if row["euro"] != "NA":
            tables_insert_commands["combined"].execute(row)
        
        issn_title_map[issn] = title
        
        if publisher != "Springer Nature":
            continue
            
        try:
            pub_year = article_pubyears[issn][doi]
        except KeyError:
            msg = ("Publication year entry not found in article cache for {}. " +
                   "You might have to update the article cache with 'python " +
                   "assets_generator.py coverage_stats'. Using the 'period' " +
                   "column for now.")
            print colorise(msg.format(doi), "yellow")
            pub_year = row["period"]
        
        if publisher not in summarised_offsetting:
            summarised_offsetting[publisher] = {}
        if issn not in summarised_offsetting[publisher]:
            summarised_offsetting[publisher][issn] = {}
        if pub_year not in summarised_offsetting[publisher][issn]:
            summarised_offsetting[publisher][issn][pub_year] = 1
        else:
            summarised_offsetting[publisher][issn][pub_year] += 1
    if institution_key_errors:
        print "KeyError: The following institutions were not found in the institutions_offsetting file:"
        for institution in institution_key_errors:
            print institution
        sys.exit()
    for publisher, issns in summarised_offsetting.iteritems():
        for issn, pub_years in issns.iteritems():
            for pub_year, count in pub_years.iteritems():
                    row = {
                        "publisher": publisher,
                        "journal_full_title": issn_title_map[issn],
                        "period": pub_year,
                        "is_hybrid": "TRUE",
                        "num_offsetting_articles": count
                    }
                    try:
                        stats = journal_coverage[issn][pub_year]
                        row["num_journal_total_articles"] = stats["num_journal_total_articles"]
                        row["num_journal_oa_articles"] = stats["num_journal_oa_articles"]
                    except KeyError as ke:
                        msg = ("KeyError: No coverage stats found for journal '{}' " +
                               "({}) in the {} period. Update the crossref cache with " +
                               "'python assets_generator.py crossref_stats'.")
                        print colorise(msg.format(issn_title_map[issn], issn, pub_year), "red")
                        sys.exit()
                    tables_insert_commands["offsetting_coverage"].execute(row)
    
    institution_countries = {}
    
    reader = UnicodeReader(open("static/institutions.csv", "rb"))
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
    
    reader = UnicodeReader(open(apc_file_name, "rb"))
    for row in reader:
        institution = row["institution"]
        # colons cannot be escaped in URL queries to the cubes server, so we have
        # to remove them here
        row["journal_full_title"] = row["journal_full_title"].replace(":", "")
        row["country"] = institution_countries[institution]
        tables_insert_commands[institution].execute(row)
        tables_insert_commands["openapc"].execute(row)
        tables_insert_commands["combined"].execute(row)

def generate_model_file(path):
    content = u""
    with open("static/templates/MODEL_FIRST_PART", "r") as model:
        content += model.read()
        
    with open("static/templates/MODEL_CUBE_STATIC_PART", "r") as model:
        static_part = model.read()

    reader = UnicodeReader(open("static/institutions.csv", "rb"))
    for row in reader:
        content += u"        ,\n        {\n"
        content += u'            "name": "{}",\n'.format((row["institution_cubes_name"]))
        content += u'            "label": "{} openAPC data cube",\n'.format((row["institution_full_name"]))
        content += static_part
        
    with open("static/templates/MODEL_LAST_PART", "r") as model:
        content += model.read()
    
    output_file = os.path.join(path, "model.json")
    with open(output_file, "w") as model:
        model.write(content.encode("utf-8"))
        
def generate_yamls(path):
    with open("static/templates/YAML_STATIC_PART", "r") as yaml:
        yaml_static = yaml.read()
    
    reader = UnicodeReader(open("static/institutions.csv", "rb"))
    for row in reader:
        content = u"name: " + row["institution_full_name"] + u"\n"
        content += u"slug: " + row["institution_cubes_name"] + u"\n"
        content += u"tagline: " + row["institution_full_name"] + u" APC data\n"
        content += u"source: Open APC\n"
        content += u"source_url: https://github.com/OpenAPC/openapc-de\n"
        content += u"data_url: https://github.com/OpenAPC/openapc-de/blob/master/data/apc_de.csv\n"
        content += u"continent: " + row["continent"] + u"\n"
        content += u"country: " + row["country"] + u"\n"
        content += u"state: " + row["state"] + u"\n"
        content += u"level: kommune\n"
        content += u"dataset: '" + row["institution_cubes_name"] + "'\n"
        content += yaml_static
        
        out_file_name = row["institution_cubes_name"] + ".yaml"
        out_file_path = os.path.join(path, out_file_name)
        with open(out_file_path, "w") as outfile:
            outfile.write(content.encode("utf-8"))


if __name__ == '__main__':
    main()
