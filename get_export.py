
import argparse
import sys
import os
import time
from tqdm import tqdm
from tabular_to_sql import convert


def get_export(client, qid, qversion, exportpath=None):

    if not exportpath:
        exportpath = "."

    qidentity = qid + "$" + str(qversion)

    response = client.export.get_info(qidentity)
    if not response['HasExportedFile']:
        _ = client.export.start(qidentity)
        response = client.export.get_info(qidentity)
        pbar = tqdm(total=100, unit="%")
        oldval = 0
        while response["RunningProcess"]:
            time.sleep(1)
            delta = response["RunningProcess"]["ProgressInPercents"] - oldval
            oldval = response["RunningProcess"]["ProgressInPercents"]
            pbar.update(delta)
            response = client.export.get_info(qidentity)
        pbar.update(100-oldval)
        pbar.close()

    zipfile = client.export.get(qidentity, exportpath=exportpath)
    docobj = client.questionnaires.document(qid, qversion)

    filename = os.path.basename(zipfile)
    filename, _ = os.path.splitext(filename)
    conn_url = "sqlite:///" + os.path.join(exportpath, filename + ".sqlite")
    convert(zipfile, conn_url, docobj)

def process_args():
    parser = argparse.ArgumentParser()
    parser.add_argument("url", help="URL of your Headquarters app")
    parser.add_argument("username", help="API user name")
    parser.add_argument("password",help="API user password")
    parser.add_argument("--qid", help="Questionnaire Id")
    parser.add_argument("--qversion", help="Questionnaire Version")
    parser.add_argument("--exportpath", help="Destination folder")

    return parser.parse_args()

def get_questionnaire(client):
    qlist = []
    i = 0
    for q in client.questionnaires.get_list():
        i += 1
        print("[{i}] {title}, version {version}".format(i=i, title=q.title, version=q.version))
        qlist.append(q)

    print()
    msg = "Pick a number between 1 and {} to download questionnaire, 'q' to exit: ".format(i)
    while True:
        choice = input(msg)
        if choice == "q":
            sys.exit()
        try:
            choice = int(choice)
        except:
            continue
        if 1 <= choice <= i:
            break
    return qlist[choice-1]

if __name__ == "__main__":
    args = process_args()

    from ssaw.headquarters import Headquarters

    client = Headquarters(args.url, args.username, args.password)
    if args.qid is None:
        q = get_questionnaire(client)
        print()
        get_export(client, q.questionnaire_id, q.version, args.exportpath)
    else:
        get_export(client, args.qid, args.version, args.exportpath)
