# -*- coding: utf-8 -*-

DESCRIPTION = """convert sql scripts from openalex-maint server to dags"""

import sys, os, time
import shutil
import re
from typing import Union
from pathlib import Path
from datetime import datetime
from timeit import default_timer as timer

try:
    from humanfriendly import format_timespan
except ImportError:

    def format_timespan(seconds):
        return "{:.2f} seconds".format(seconds)


import logging

root_logger = logging.getLogger()
logger = root_logger.getChild(__name__)


def process_sql_file(sql_code: str) -> str:
    outlines = []
    ignore_lines = [
        r"\timing",
    ]
    for line in sql_code.splitlines():
        if line and line not in ignore_lines:
            outlines.append(line)
    return '\n'.join(outlines)

def process_line(
    line: str,
    dirpath_sqlfiles: Path,
    outdir_sqlfiles: Path,
    outdir_dags: Path,
    dag_template: str,
) -> Union[Path, None]:
    p = re.compile(
        r"""(\S+ +\S+ +\S+ +\S+ +\S+) .*?echo "(.*?)".*?psql .*?\s(\S*?\.sql)"""
    )
    m = p.search(line)
    if m:
        cron_expr = m.group(1)
        name = m.group(2)
        sql_fp = Path(m.group(3))
        sql_fname = sql_fp.name
        if name == "unstick queues":
            # skip for now
            return None
        sqlfile_matches = list(dirpath_sqlfiles.glob(f"*{sql_fname}"))
        if not sqlfile_matches:
            return None
        elif len(sqlfile_matches) > 1:
            raise RuntimeError()
        sqlfile_match = sqlfile_matches[0]
        dest = outdir_sqlfiles.joinpath(sql_fname)
        dest.write_text(process_sql_file(sqlfile_match.read_text()))
        dag_name = f"""{name.replace(' ', '_')}_dag"""
        dag_code = dag_template.replace("REPLACE_WITH_CRONTAB_EXPRESSION", cron_expr)
        dag_code = dag_code.replace("REPLACE_WITH_DAG_NAME", dag_name)
        dag_code = dag_code.replace("REPLACE_WITH_SQL_SCRIPT_FILENAME", sql_fname)
        outfname = f"{dag_name}.py"
        outfp = outdir_dags.joinpath(outfname)
        outfp.write_text(dag_code)
        return outfp
    return None


def main(args):
    input_fp = Path(args.input)
    dirpath_sqlfiles = Path(args.dirname_sqlfiles)
    outdir_sqlfiles = Path(args.output_sqlfiles)
    outdir_dags = Path(args.output_dags)
    dag_template = Path(args.dag_template).read_text()
    crontab_txt = input_fp.read_text()
    dag_files_written = []
    for line in crontab_txt.split("\n"):
        if "psql $OPENALEX_DB" in line and not line.startswith("#"):
            fp = process_line(
                line, dirpath_sqlfiles, outdir_sqlfiles, outdir_dags, dag_template
            )
            if fp:
                dag_files_written.append(fp)
                logger.debug(f"created dag: {fp}")
    logger.info(f"finished. created {len(dag_files_written)} dags")


if __name__ == "__main__":
    total_start = timer()
    handler = logging.StreamHandler()
    handler.setFormatter(
        logging.Formatter(
            fmt="%(asctime)s %(name)s.%(lineno)d %(levelname)s : %(message)s",
            datefmt="%H:%M:%S",
        )
    )
    root_logger.addHandler(handler)
    root_logger.setLevel(logging.INFO)
    logger.info(" ".join(sys.argv))
    logger.info("{:%Y-%m-%d %H:%M:%S}".format(datetime.now()))
    logger.info("pid: {}".format(os.getpid()))
    import argparse

    parser = argparse.ArgumentParser(description=DESCRIPTION)
    parser.add_argument("input", help="input filename (crontab file)")
    parser.add_argument(
        "--dirname-sqlfiles",
        default="../include/openalex-maint",
        help="directory to look for sql files",
    )
    parser.add_argument(
        "--output-sqlfiles", default="../include", help="directory to output sql files"
    )
    parser.add_argument(
        "--output-dags", default="../dags/", help="directory to output dag files (.py)"
    )
    parser.add_argument(
        "--dag-template",
        default="./dag_template.py",
        help="filename for dag template (.py)",
    )
    parser.add_argument("--debug", action="store_true", help="output debugging info")
    global args
    args = parser.parse_args()
    if args.debug:
        root_logger.setLevel(logging.DEBUG)
        logger.debug("debug mode is on")
    main(args)
    total_end = timer()
    logger.info(
        "all finished. total time: {}".format(format_timespan(total_end - total_start))
    )
