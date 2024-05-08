import os
from pathlib import Path
from typing import List, Tuple, Union, Optional

from loguru import logger

from cmdline.run_cmd import run_cmd


class GitRepo:
    PATCH_FAILED_ERROR = "patch failed"

    def __init__(self, repo: Path):

        self.repo: Path = repo

    def git_cmd(self, cmd: List[str], entry: Union[str, None]) -> Tuple[str, str]:
        cwd = os.getcwd()
        os.chdir(self.repo)
        cmd = ['git'] + cmd + [entry] if entry else ['git'] + cmd
        std, err = run_cmd(cmd=cmd)
        os.chdir(cwd)
        return std, err

    def git_pull(self) -> Tuple[str, str]:
        return self.git_cmd(cmd=['pull'], entry=None)

    def git_pull_from_origin(self, branch: str) -> Tuple[str, str]:
        return self.git_cmd(cmd=['pull', 'origin'], entry=branch)

    def git_checkout(self, entry: str) -> Tuple[str, str]:
        return self.git_cmd(cmd=['checkout'], entry=entry)

    def git_apply_patch(self, entry: str):
        # apply helm.patch  apply --reject --whitespace=fix
        std_out, std_err = self.git_cmd(cmd=['apply'], entry=entry)
        # check for error: patch failed might be different git version
        if self.PATCH_FAILED_ERROR in std_err:
            msg = f"{self.git_apply_patch.__name__} failed with {std_err}. Try with --whitespace=fix"
            logger.info(msg)
            std_out, std_err = self.git_cmd(cmd=['apply', '--whitespace=fix'], entry=entry)
            cond: bool = self.PATCH_FAILED_ERROR in std_err
            assert not cond

    def prepare_git_repo(self, branch: str, patches: Optional[List[Path]] = None):
        """Apply patches from list of files in folder"""
        if patches is None:
            patches = []
        msg = f"Preparing git repo {self.repo}"
        logger.info(msg)
        self.git_checkout(entry='.')
        self.git_checkout(entry=branch)
        self.git_pull_from_origin(branch=branch)
        for patch in patches:
            self.git_apply_patch(entry=str(patch))

    def add_notes(self, notes: str, chart: str = "ataccama-one/charts/ataccama-one/templates"):
        """
        Create file NOTES.txt from notes in chart folder. Default value main ataccama-one template
        Added as NOTES: node to manifest.yaml
        """
        with open(Path(self.repo, chart, 'NOTES.txt'), "w") as notes_file:
            notes_file.write(notes)
