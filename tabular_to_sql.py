import argparse
import zipfile
from tempfile import TemporaryDirectory
from pathlib import Path
import csv
from sqlalchemy import create_engine, Table, Column, Integer, String, Float, MetaData, PrimaryKeyConstraint

from ssaw.designer import import_questionnaire_json

def create_schema(variablenames, variables, roster_id = None):
    ret = []
    for var in variablenames:
        if hasattr(variables, var):
            t = variables[var]._Type
        else:
            t = "TextQuestion"
        if t == 'SingleQuestion':
            type = Integer
        elif t == 'NumericQuestion':
            if variables[var].IsInteger:
                type =  Integer
            else:
                type = Float
        elif var == roster_id:
            type = Integer
        else:
            type = String
        ret.append({"Name": var, "Type": type})
    return ret

def create_table(tablename, schema, keys, metadata):
    columns = [Column(col["Name"], col["Type"]) for col in schema]

    _ = Table(tablename, metadata, *columns,
              PrimaryKeyConstraint(*keys, name=tablename + '_pk'))

def read_header(filepath):
    with open(filepath, "r", encoding="utf-8-sig") as f:
        return f.readline().replace("\n", "").split("\t")

def fix_dtypes(d, sc):
    for col in sc:
        varname = col["Name"]
        if d[varname] in ["-999999999", "##N/A##"]:
            d[varname] = None
        if d[varname]:
            if col["Type"] == Integer:
                d[varname] = int(d[varname])
            elif col["Type"] == Float:
                d[varname] = float(d[varname])
        else:
            d[varname] = None
    return d

TABLES = {
    "assignment__actions": {
        "schema":[
            {"Name": "assignment__id", "Type": Integer},
            {"Name": "date", "Type": String},
            {"Name": "time", "Type": String},
            {"Name": "action", "Type": Integer},
            {"Name": "originator", "Type": String},
            {"Name": "role", "Type": Integer},
            {"Name": "responsible__name", "Type": String},
            {"Name": "responsible__role", "Type": Integer},
            {"Name": "old__value", "Type": String},
            {"Name": "new__value", "Type": String},
            {"Name": "comment", "Type": String}
        ],
        "keys":["assignment__id", "action"]
    },
    "interview__actions": {
        "schema":[
            {"Name": "interview__key", "Type": String},
            {"Name": "interview__id", "Type": String},
            {"Name": "date", "Type": String},
            {"Name": "time", "Type": String},
            {"Name": "action", "Type": Integer},
            {"Name": "originator", "Type": String},
            {"Name": "role", "Type": Integer},
            {"Name": "responsible__name", "Type": String},
            {"Name": "responsible__role", "Type": Integer},
        ],
        "keys":[]
    },
    "interview__comments": {
        "schema":[
            {"Name": "interview__key", "Type": String},
            {"Name": "interview__id", "Type": String},
            {"Name": "roster", "Type": String},
            {"Name": "id1", "Type": String},
            {"Name": "variable", "Type": String},
            {"Name": "order", "Type": String},
            {"Name": "date", "Type": String},
            {"Name": "time", "Type": String},
            {"Name": "originator", "Type": String},
            {"Name": "role", "Type": Integer},
            {"Name": "comment", "Type": String},
        ],
        "keys":[]
    },
    "interview__diagnostics": {
        "schema":[
            {"Name": "interview__key", "Type": String},
            {"Name": "interview__id", "Type": String},
            {"Name": "interview__status", "Type": Integer},
            {"Name": "responsible", "Type": String},
            {"Name": "interview__status", "Type": Integer},
            {"Name": "interviewers", "Type": Integer},
            {"Name": "rejections__sup", "Type": Integer},
            {"Name": "rejections__hq", "Type": Integer},
            {"Name": "entities__errors", "Type": Integer},
            {"Name": "questions__comments", "Type": Integer},
            {"Name": "interview__duration", "Type": Float},
        ],
        "keys":[]
    },
    "interview_errors": {
        "schema":[
            {"Name": "interview__key", "Type": String},
            {"Name": "interview__id", "Type": String},
            {"Name": "roster", "Type": String},
            {"Name": "id1", "Type": String},
            {"Name": "variable", "Type": String},
            {"Name": "type", "Type": String},
            {"Name": "message__number", "Type": Integer},
            {"Name": "message", "Type": String},
        ],
        "keys":[]
    },
}

def convert(sourcezip, conn_url, document):

    with open(document, 'r', encoding="utf-8-sig") as f:
        variables = import_questionnaire_json(f.read()).variables()

    engine = create_engine(conn_url)
    metadata = MetaData()

    with TemporaryDirectory() as tempdir:
        with zipfile.ZipFile(sourcezip, 'r') as zip_ref:
            zip_ref.extractall(tempdir)
        for f in Path(tempdir).glob('*.tab'):
            tablename = f.name.replace(".tab", "")
            if tablename in TABLES:
                sc = TABLES[tablename]["schema"]
                keys = TABLES[tablename]["keys"]
            else:
                columns = read_header(f)
                keys = ["interview__key", "interview__id"]
                roster_id = tablename + "__id" 
                if roster_id in columns:
                    keys = keys + [roster_id,]
                else:
                    roster_id = None
                sc = create_schema(columns, variables, roster_id)
                TABLES[tablename] = {"schema": sc, "keys": keys}
            create_table(tablename, sc, keys, metadata)
        
        metadata.drop_all(engine)
        metadata.create_all(engine)

        for f in Path(tempdir).glob('*.tab'):
            tablename = f.name.replace(".tab", "")
            ins = metadata.tables[tablename].insert()
            with open(f, "r", encoding="utf-8-sig") as f:
                rd = csv.DictReader(f, delimiter='\t')
                for row in rd:
                    engine.execute(ins, fix_dtypes(row,TABLES[tablename]["schema"]))

if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("sourcezip",
        help="path to exported zip file with tabular data")
    parser.add_argument(
        "document", help="json file containing questionnaire document")
    parser.add_argument("conn_url",
        help="connection string to the destination database")
    args = parser.parse_args()

    convert(args.sourcezip, args.conn_url, args.document)
