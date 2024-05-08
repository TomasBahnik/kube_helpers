import logging
import os
import re
import socket
import subprocess
from datetime import datetime, timezone
from pathlib import Path
from typing import List, Optional, Set

import typer

from loguru import logger

DATE_TIME_FORMAT_FOLDER = "%Y-%m-%dT%H-%M-%S%z"

START_ITER_REGEXP = r"Starting\siteration\s(\d+)"
START_ITER = re.compile(START_ITER_REGEXP)

NOTIFY_TRANSACTION = 'Notify: Transaction'
START_TRX = re.compile(NOTIFY_TRANSACTION + r"\s\"(.*)\"\sstarted")
# ended with a "Pass" status (Duration: 0.8570 Wasted Time: 0.3640)
TRX_COMMON = r"\s\"(.*)\"\sended\swith\sa\s\"(.*)\"\sstatus"
END_TRX = re.compile(NOTIFY_TRANSACTION + TRX_COMMON)
END_TRX_PASSED_DURATION = re.compile(r".*\(Duration:\s(\d+\.\d+)\)")
END_TRX_PASSED_DURATION_WASTED = re.compile(r".*\(Duration:\s(\d+\.\d+)\sWasted\sTime:\s(\d+\.\d+)\)")
END_TRX_PASSED_DURATION_THINK = re.compile(r".*\(Duration:\s(\d+\.\d+)\sThink\sTime:\s(\d+\.\d+)\)")
# (Duration: 14.7360 Think Time: 0.0010 Wasted Time: 3.5380)
END_TRX_PASSED_ALL_TIMES = \
    re.compile(r".*\(Duration:\s(\d+\.\d+)\sThink\sTime:\s(\d+\.\d+)\sWasted\sTime:\s(\d+\.\d+)\)")
# t=00006915ms
LOG_TIMESTAMP_RE = re.compile(r"t=(\d+)ms")
# Virtual User Script started at : 2020-10-05 11:27:34
SCRIPT_STARTED_RE = re.compile(r"Virtual User Script.+(\d{4}-\d{2}-\d{2}\s\d{2}:\d{2}:\d{2})")
# {"operationName":"GetApplicationMode","variables":{}
# TODO operationName -> OPERATION_NAME const can't escape '\' in f strings use """?
# OPERATION_NAME_START = r"{\"operationName\":\"(\w+)\",\"variables\":(\{.*\})"
OPERATION_NAME_START = r"{\"operationName\":\"(\w+)\","
OPERATION_NAME_START_RE = re.compile(OPERATION_NAME_START)
RUNTIME_SETTINGS_FILE_RE = re.compile(r"Run-Time Settings file.+: \"(.+)\\default.cfg")
# t=00005289ms: Request headers for "https://one-nbcxj.worker-01-euc1.prod.ataccama.link/" (465 byte(s))
FE_APP_URL_RE = re.compile(r"t=\d+ms: Request headers for \"(.+\.\w+/)\" \(")

# load steps and actions from output.txt
# used as LOG_TIMESTAMP_RE = re.compile(r"t=(\d+)ms")
# t=00029357ms: Step 21: Click on Browse button started    [MsgId: MMSG-205180]	[MsgId: MMSG-205180]
SCRIPT_STEP_OUTPUT_RE = r't=(\d+)ms:\sStep\s([\d\.]+):\s([a-zA-Z\s"]+[\w"])\s{4}'
SCRIPT_STEP_OUTPUT_RE_COMPILED = re.compile(SCRIPT_STEP_OUTPUT_RE)


def list_files(folder: Path, ends_with: str = '.txt', contains: Optional[str] = None) -> List[Path]:
    """
    traverse root directory, and list directories as dirs and files as files
    finds files or roots which contain `contains` string and ends_with `ends_with` string
    """
    file_type_files: List[Path] = []
    for root, dirs, files in os.walk(folder):
        for file in files:
            cond_ends = file.endswith(ends_with)
            cond_contains = True if contains is None else (contains in file or contains in root)
            condition = cond_ends and cond_contains
            if condition:
                l_f = Path(root, file)
                file_type_files.append(l_f)
    return file_type_files


def files_containing(files: List[Path] | Set[Path], contains: str):
    ret: List[Path] = []
    for file in files:
        #  for windows utf-8 explicitly
        f = open(file=file, encoding='utf-8', mode='r')
        if contains in f.read():
            ret.append(file)
        f.close()
    return ret


def time_stamp(date_time_format: str = DATE_TIME_FORMAT_FOLDER) -> str:
    """ default format suitable for directory name"""
    return datetime.now(timezone.utc).strftime(date_time_format)


def archive_folder(src_path: Path, dest_path: Path, dest_base_file_name: str):
    """
    Create tar gz from 2nd to last dir of src_folder
    archive file name = dest_file_name + datatime stamp + suffix
    """
    dt = time_stamp()
    src_parts = src_path.parts
    last_folder = src_parts[-1]
    path_before_last_folder = Path(*src_parts[:-1])
    os.makedirs(dest_path, exist_ok=True)
    dest_file_name = Path(str(dest_base_file_name) + "-" + dt + ".tar.gz")
    full_archive_path: Path = Path(dest_path, dest_file_name).resolve()
    msg = f"Archive '{last_folder}' from {path_before_last_folder} to {full_archive_path}"
    logger.info(msg)
    # -C change dir to second to last backup dir and archive only this one
    cmd = ['tar', '-C', str(path_before_last_folder), '-czf', str(full_archive_path), last_folder]
    p = subprocess.run(cmd)
    return p


MMM_BE_BUILD_INFO_KEY = "mmmBeBuildInfo"
MMM_BUILD_VERSION_KEY = "buildVersion"
TIMESTAMP_KEY = 'timeStamp'
SOURCE_DB_KEY = 'sourceDb'
FROM_DATE_KEY = 'fromDate'
# used as filter for MMM/DPM part of job stats
DPM_SOURCE_DB = 'dpm'
MMM_SOURCE_DB = 'mmm'
# keys in job stat json
DPM_JOB_STATS_KEY = f'{DPM_SOURCE_DB}_jobs'
MMM_JOB_STATS_KEY = f'{MMM_SOURCE_DB}_jobs'


def check_folder(folder: Path) -> Path:
    if folder.exists() and folder.is_dir():
        return folder
    else:
        raise IsADirectoryError(folder)


def check_file(file: Path) -> Path:
    if file.exists() and file.is_file():
        return file
    else:
        raise FileNotFoundError(file)


def os_info():
    """OS and user info"""
    try:
        ret = f"OS:{os.name} host:{socket.gethostname()} user:{os.getlogin()}"
        return ret
    # On WSL os.getlogin() return FileNotFoundError: [Errno 2] No such file or directory
    except FileNotFoundError:
        return f"OS:{os.name} host:{socket.gethostname()}"


def log_console(message, current_logger: logging.Logger = None, error: bool = False):
    typer.echo(message=f'{time_stamp()} : {message}')
    if current_logger is not None:
        if error:
            current_logger.error(msg=message)
        else:
            current_logger.info(msg=message)


def find_chars_in_str(s: str, ch) -> List[int]:
    """
    Find positions of single char is string
    https://stackoverflow.com/questions/11122291/how-to-find-char-in-string-and-get-all-the-indexes
    :param s: string to search
    :param ch: char to search for
    :return: list of indexes of ch in s
    """
    return [i for i, ltr in enumerate(s) if ltr == ch]


def basic_auth(username: str, password: str) -> str:
    import base64
    auth = f'{username}:{password}'
    auth_bytes = auth.encode('utf-8')
    auth_b64 = base64.b64encode(auth_bytes)
    ret = f"Basic {auth_b64.decode('utf-8')}"
    return ret
