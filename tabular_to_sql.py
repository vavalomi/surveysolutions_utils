import argparse
import zipfile
from tempfile import TemporaryDirectory
from pathlib import Path
import csv
from datetime import datetime
from sqlalchemy import create_engine, MetaData, Table, PrimaryKeyConstraint, Column, Integer, String, Float, Date, Time, DateTime
from tqdm import tqdm

from ssaw.designer import import_questionnaire_json


def create_schema(variablenames, variables, roster_id = None):
    ret = []
    for var in variablenames:
        var_val = var.split("__")
        if var_val[0] in variables:
            t = variables[var_val[0]]._Type
        else:
            t = "TextQuestion"
        if t in ['SingleQuestion', 'MultyOptionsQuestion']:
            type = Integer
        elif t == "NumericQuestion":
            if variables[var].IsInteger:
                type =  Integer
            else:
                type = Float
        elif t == "GpsCoordinateQuestion":
            if var_val[1] == "Timestamp":
                type = DateTime
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
        if varname not in d:
            continue
        if d[varname] in ["-999999999", "##N/A##"]:
            d[varname] = None
        if d[varname]:
            if col["Type"] == Integer:
                d[varname] = int(d[varname])
            elif col["Type"] == Float:
                d[varname] = float(d[varname])
            elif col["Type"] == Date:
                d[varname] = datetime.strptime(d[varname], '%Y-%m-%d')
            elif col["Type"] == Time:
                d[varname] = datetime.strptime(d[varname], '%H:%M:%S').time()
            elif col["Type"] == DateTime:
                d[varname] = datetime.strptime(d[varname], '%Y-%m-%dT%H:%M:%S')
        else:
            d[varname] = None
    return d

TABLES = {
    "assignment__actions": {
        "schema":[
            {"Name": "assignment__id", "Type": Integer},
            {"Name": "date", "Type": Date},
            {"Name": "time", "Type": Time},
            {"Name": "action", "Type": Integer},
            {"Name": "originator", "Type": String},
            {"Name": "role", "Type": Integer},
            {"Name": "responsible__name", "Type": String},
            {"Name": "responsible__role", "Type": Integer},
            {"Name": "old__value", "Type": String},
            {"Name": "new__value", "Type": String},
            {"Name": "comment", "Type": String}
        ],
        "keys":["assignment__id", "date", "time", "action"]
    },
    "interview__actions": {
        "schema":[
            {"Name": "interview__key", "Type": String},
            {"Name": "interview__id", "Type": String},
            {"Name": "date", "Type": Date},
            {"Name": "time", "Type": Time},
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
            {"Name": "interview__duration", "Type": String},
        ],
        "keys":[]
    },
    "interview__errors": {
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

def no_data(file):
    with open(file, 'r') as f:
        header = f.readline()
        if f.readline():
            return False
        else:
            return True

def convert(sourcezip, conn_url, docobj):

    variables = docobj.variables()

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
                keys = ["interview__id",]
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
            if no_data(f):
                continue
            ins = metadata.tables[tablename].insert()
            with open(f, "r", encoding="utf-8-sig") as f:
                rd = csv.DictReader(f, delimiter='\t')
                pbar =tqdm(rd, unit=' rows')
                pbar.set_description("Processing %s" % tablename)
                engine.execute(ins,[fix_dtypes(row, TABLES[tablename]["schema"]) for row in pbar])

def process_args():
    parser = argparse.ArgumentParser()
    parser.add_argument("sourcezip",
        help="path to exported zip file with tabular data")
    parser.add_argument(
        "document", help="json file containing questionnaire document")
    parser.add_argument("conn_url",
        help="connection string to the destination database")
    return parser.parse_args()

if __name__ == "__main__":
    args = process_args()

    with open(args.document, 'r', encoding="utf-8-sig") as f:
        docobj = import_questionnaire_json(f.read())
    convert(args.sourcezip, args.conn_url, docobj)
