import logging
import subprocess
from pathlib import Path
from typing import Tuple, List

import typer

logger = logging.getLogger(__name__)
MAX_STD_OUT_LEN = 2000


def run_cmd(cmd: List[str]) -> Tuple[str, str]:
    msg = f"Running {cmd}"
    typer.echo(message=msg)
    logger.info(msg=msg)
    p = subprocess.run(cmd, stdout=subprocess.PIPE, stderr=subprocess.PIPE)
    std_out = p.stdout.decode('utf-8').strip()
    std_err = p.stderr.decode('utf-8').strip()
    m = msg if msg is not None else str(cmd)
    logger.info(f"{m} : stdout = {std_out[:MAX_STD_OUT_LEN]}")
    if std_err and len(std_err) > 0:
        msg = f"{m} : stderr = {std_err}"
        typer.echo(f"{'=' * 8} std ERROR start{'=' * 8}")
        typer.echo(message=msg)
        typer.echo(f"{'=' * 8} std ERROR end{'=' * 8}")
        logger.error(f"{m} : stderr = {std_err}")
    return std_out, std_err


def decoded(text: bytes):
    return text.decode('utf-8').strip()


class RunCommand:
    def __init__(self, command: List[str]):
        self.command: List[str] = command
        self.process: subprocess.CompletedProcess = subprocess.run(self.command,
                                                                   stdout=subprocess.PIPE, stderr=subprocess.PIPE)
        self.stdout = self.process.stdout
        self.stdout_decoded = decoded(self.process.stdout)
        self.stderr = self.process.stderr
        self.stderr_decoded = decoded(self.process.stderr)
        self.return_code = self.resolve_return_code()

    def __str__(self):
        return str(self.command)

    def error_msg(self) -> str:
        msg = f"{self}: ret code {self.return_code}, stdout {self.stdout_decoded}, stderr {self.stderr_decoded}"
        return msg

    def resolve_return_code(self):
        """Even if return code = 0 stderr might NOT be empty (psql)
           keep return code unchanged if there is empty stderr
        """
        err_len = len(self.stderr)
        ret_code = self.process.returncode
        return err_len if err_len > 0 and ret_code == 0 else ret_code


def grep_file(file: Path, regexp: str) -> RunCommand:
    logger.info(f"Searching for '{regexp}' in {file}")
    # grep returns 1 when the regexp IS NOT FOUND - i.e. non-zero status ! => DO NOT set -e
    grep_error = ['grep', f"{regexp}", str(file)]
    cmd: RunCommand = RunCommand(command=grep_error)
    return cmd
