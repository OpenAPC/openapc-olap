#!/usr/bin/env python

import argparse
import csv
import codecs
import ConfigParser
import os
import sys

import sqlalchemy

# These two classes were adopted from 
# https://docs.python.org/2/library/csv.html#examples
# UnicodeReader was slightly modified to return a DictReader
class UTF8Recoder:
    """
    Iterator that reads an encoded stream and reencodes the input to UTF-8
    """
    def __init__(self, f, encoding):
        self.reader = codecs.getreader(encoding)(f)

    def __iter__(self):
        return self

    def next(self):
        return self.reader.next().encode("utf-8")
        
class UnicodeReader(object):
    """
    A CSV reader which will iterate over lines in the CSV file "f",
    which is encoded in the given encoding.
    """

    def __init__(self, f, dialect=csv.excel, encoding="utf-8", **kwds):
        f = UTF8Recoder(f, encoding)
        self.reader = csv.DictReader(f, dialect=dialect, **kwds)

    def next(self):
        row = self.reader.next()
        return {k: unicode(v, "utf-8") for (k, v) in row.iteritems()}

    def __iter__(self):
        return self
        
ARG_HELP_STRINGS = {
    
    "dir": "A path to a directory where the generated output files should be stored. " +
           "If omitted, output will be written to the current directory."
}

def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("job", choices=["tables", "model", "yamls", "db_settings"])
    parser.add_argument("-d", "--dir", help=ARG_HELP_STRINGS["dir"])
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
        create_cubes_tables(engine, "apc_de.csv", "offsetting_demo.csv")
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
        ("country", "string")
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
    
    # a dict to store individual insert commands for every table
    tables_insert_commands = {
        "openapc": openapc_insert_command,
        "offsetting": offsetting_insert_command,
        "combined": combined_insert_command
    }
    
    offsetting_institution_countries = {}
    
    reader = UnicodeReader(open("static/institutions_offsetting.csv", "rb"))
    for row in reader:
        institution_name = row["institution"]
        country = row["country"]
        offsetting_institution_countries[institution_name] = country
        
    reader = UnicodeReader(open(offsetting_file_name, "rb"))
    for row in reader:
        institution = row["institution"]
        # colons cannot be escaped in URL queries to the cubes server, so we have
        # to remove them here
        row["journal_full_title"] = row["journal_full_title"].replace(":", "")
        try:
            row["country"] = offsetting_institution_countries[institution]
        except KeyError as ke:
            msg = (u"KeyError: The institution '{}' was not found in the institutions_offsetting file!")
            print msg.format(institution)
            sys.exit()
        tables_insert_commands["offsetting"].execute(row)
        if row["euro"] != "NA":
            tables_insert_commands["combined"].execute(row)
    
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
