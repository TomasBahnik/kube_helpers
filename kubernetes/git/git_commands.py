import os
import subprocess
from pathlib import Path


def git_last_file_change(git_dir: Path, rel_path: Path):
    cwd = os.getcwd()
    os.chdir(Path(git_dir))
    git_log = ['git', 'log', '-p', '-1', '--', str(rel_path)]
    grep_date = ['grep', 'Date']
    p1 = subprocess.Popen(git_log, stdout=subprocess.PIPE, stderr=subprocess.PIPE)
    p2 = subprocess.run(grep_date, stdin=p1.stdout, stdout=subprocess.PIPE)
    last_modified: str = p2.stdout.decode('utf-8').strip()
    os.chdir(cwd)
    return last_modified
