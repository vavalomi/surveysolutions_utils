
import argparse
import os
import sys
import time

from ssaw import Client, ExportApi, QuestionnairesApi
from ssaw.models import ExportJob
from ssaw.utils import to_qidentity

from tqdm import tqdm


def get_export(client, qid, qversion, exporttype, interviewstatus, exportpath=None):

    if not exportpath:
        exportpath = "."

    isfile = os.path.isfile(exportpath)

    qidentity = to_qidentity(qid, qversion)

    args = {
        "questionnaire_identity": qidentity,
        "export_type": exporttype,
        "interview_status": interviewstatus,
    }

    response = ExportApi(client).get(**args, export_path= exportpath)
    if not response:
        job = ExportJob(**args)
        job = ExportApi(client).start(job)
        job = ExportApi(client).get_info(job.job_id)
        pbar = tqdm(total=100, unit="%")
        oldval = 0
        while job.export_status != "Completed":
            time.sleep(1)
            delta = job.progress - oldval
            oldval = job.progress
            pbar.update(delta)
            job = ExportApi(client).get_info(job.job_id)
        if job.has_export_file:
            response = ExportApi(client).get(**args, export_path= exportpath)

        pbar.update(100 - oldval)
        pbar.close()

    print("Archive was downloaded to {}".format(response))


def process_args():
    parser = argparse.ArgumentParser(add_help=False)
    parser.add_argument("--url", required=True, help="URL of your Headquarters app")
    parser.add_argument("--username", required=True, help="API user name")
    parser.add_argument("--password", required=True, help="API user password")

    parser.add_argument("--qid", required=False, help="Questionnaire Id")
    parser.add_argument("--qversion", default=1, help="Questionnaire Version")
    parser.add_argument("--exporttype", default="Tabular", help="Export file format (default: Tabular)")
    parser.add_argument("--interviewstatus", default="All", help="Include interview statuses (default: All)")
    parser.add_argument("--exportpath", help="Destination folder")

    return parser.parse_args()


def get_questionnaire(client):
    qlist = []
    i = 0
    for q in QuestionnairesApi(client).get_list():
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
        except TypeError:
            continue
        if 1 <= choice <= i:
            break
    return qlist[choice - 1]


if __name__ == "__main__":
    args = process_args()

    client = Client(args.url, args.username, args.password)
    expargs = {
        "client": client,
        "exportpath": args.exportpath,
        "exporttype": args.exporttype,
        "interviewstatus": args.interviewstatus
    }
    if args.qid is None:
        q = get_questionnaire(client)
        print()
        get_export(qid=q.questionnaire_id, qversion=q.version,**expargs)
    else:
        get_export(qid=args.qid, qversion=args.qversion, **expargs)
