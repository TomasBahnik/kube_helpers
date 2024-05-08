import os
import re
from pathlib import Path
from typing import Optional, List, Tuple

import typer
from loguru import logger

from cmdline.common import time_stamp
from cmdline.configuration import TestEnvProperties
from cmdline.helm import helm_commands
from cmdline.helm.values_file import HelmValuesFile
from cmdline.git import git_repo
from cmdline.redeploy import Redeploy, MINIO_BUCKETS
from cmdline.run_cmd import run_cmd
from redeploy_helm import HELM_APPLICATION_NOTES_KEY

ORIGINAL_VALUES_YAML = 'original_values.yaml'
ORIGINAL_VALUES_YAML_NO_EXTRA_DPM_PROPS = 'original_values_no_extra_props.yaml'


class RedeployPortal(Redeploy):
    def __init__(self, test_env: str, branch: Optional[str], sizing: Optional[str], modules: Optional[str],
                 multiply_resources: Optional[Tuple[str, ...]]):
        super().__init__()
        self.properties = TestEnvProperties(test_env=test_env)
        self.sizing = sizing
        self.modules = modules
        self.namespace = self.properties.app_namespace
        self.multiply_resources: Tuple[str, ...] = multiply_resources
        self.branch = None
        self.artifact_folder = None
        self.artifacts_time_stamp_folder: Optional[Path] = None
        self.init_folders(branch=branch)

    def init_folders(self, branch: Optional[str]):
        """Init and create folder for given test env, branch, namespace and timestamp"""
        if branch:
            self.branch = branch
            # branch is used as folder and also looked as str n Path, someone is used to name branch as name/xxxx
            branch_folder = self.branch.replace('/', '_')
            helm_perf_test_env_path: Path = Path(self.properties.helm_perf_values_dir, self.properties.test_env)
            # used as folder for original values file when installing
            # and for timestamp based sub-folders
            self.artifact_folder = Path(helm_perf_test_env_path, branch_folder, self.namespace)
            self.artifacts_time_stamp_folder: Optional[Path] = Path(self.artifact_folder, time_stamp())
            os.makedirs(self.artifacts_time_stamp_folder, exist_ok=True)

    def delete_app(self, properties: TestEnvProperties, helm_app_name: Optional[str], namespace: Optional[str]):
        run_cmd(helm_commands.uninstall(helm_app_name=helm_app_name, helm_ns=namespace))

    def delete_p_v(self, properties: TestEnvProperties, namespace: str):
        raise NotImplementedError

    def delete_p_v_portal(self):
        cmd = ['kubectl', 'delete', '--all', 'pvc', '-n', self.namespace]
        run_cmd(cmd)

    def build_deployment(self, properties: TestEnvProperties):
        pass

    @staticmethod
    def _build_deployment(values_files: List[str], dry_run: bool,
                          command: str, helm_app_name: str,
                          helm_ns: str) -> str:
        cmd_base = helm_commands.deploy(command=command, helm_app_name=helm_app_name, helm_ns=helm_ns, dry_run=dry_run)
        cmd = cmd_base + values_files
        std_out, std_err = run_cmd(cmd)
        return std_out

    def save_analyze_manifest(self, manifest: str):
        manifest = re.sub(r'Release.+!\n', '', manifest)
        from redeploy_helm import PERF_MANIFEST_FILE_NAME
        manifest_filename = f'{self.sizing}_{PERF_MANIFEST_FILE_NAME}'
        manifest_file_path = Path(self.artifacts_time_stamp_folder, manifest_filename)
        with open(manifest_file_path, "w", encoding='utf-8') as manifest_file:
            manifest_file.writelines(manifest)
        typer.echo(f"manifest saved to {manifest_file_path}")
        from cmdline.helm.sizing import Sizing
        sizing = Sizing(manifest_file=Path(self.artifacts_time_stamp_folder, manifest_filename))
        os.makedirs(self.artifacts_time_stamp_folder, exist_ok=True)
        typer.echo(f"Save '{self.sizing}' sizing to {self.artifacts_time_stamp_folder.resolve()}")
        sizing.save_sizing(save_path=self.artifacts_time_stamp_folder, base_file_name=f'{self.sizing}_sizing')

    def add_multiplied_resources(self, values_files_cmd_line: List[str], values_file: HelmValuesFile):
        if self.multiply_resources:
            multiplied_value_file: Path = values_file.multiply_resources(folder=self.artifacts_time_stamp_folder,
                                                                         multiply_mem=2,
                                                                         multiply_cpu=3,
                                                                         filter_components=self.multiply_resources)
            return values_files_cmd_line + ['--values', str(multiplied_value_file)]
        else:
            return values_files_cmd_line

    def portal_upgrade(self, dry_run: bool, get_values: bool):
        if not get_values:
            logger.info(f'loading {ORIGINAL_VALUES_YAML} from {self.artifact_folder}')
            orig_values_file = Path(self.artifact_folder, ORIGINAL_VALUES_YAML)
            if not orig_values_file.exists():
                raise FileExistsError(orig_values_file)
        else:
            # if folder specified artifact is saved to it and path is returned
            logger.info(f'loading {ORIGINAL_VALUES_YAML} from {self.namespace} env')
            orig_values_file: Path = helm_commands.get(command='values', helm_app_name=self.namespace,
                                                       helm_ns=self.namespace, folder=self.artifacts_time_stamp_folder,
                                                       file_name=ORIGINAL_VALUES_YAML)
        values_file: HelmValuesFile = HelmValuesFile(sizing=self.sizing, modules_config=self.modules,
                                                     use_ordered_dict=False)
        orig_manifest = helm_commands.get(command='manifest', helm_app_name=self.namespace, helm_ns=self.namespace,
                                          folder=self.artifacts_time_stamp_folder, file_name='original_manifest.yaml')
        # needed for mdmServer disabled, dpm extra properties removed locally
        values_files_cmd_line = self.all_values_files(orig_values_file, values_file)
        values_files_cmd_line = self.add_multiplied_resources(values_files_cmd_line=values_files_cmd_line,
                                                              values_file=values_file)
        self.prepare_helm_charts()
        manifest = self._build_deployment(dry_run=dry_run, command='upgrade',
                                          values_files=values_files_cmd_line, helm_app_name=self.namespace,
                                          helm_ns=self.namespace)
        if dry_run:
            self.save_analyze_manifest(manifest=manifest)

    def portal_install(self, dry_run: bool):
        """ deployment does not exist no helm get possible  - use stored original file
            which is expected at location test_env/chart_version/namespace/original_values_no_extra_props.yaml
        """
        orig_values_file = Path(self.artifact_folder, ORIGINAL_VALUES_YAML)
        if not orig_values_file.exists():
            raise FileExistsError(orig_values_file)
        values_file: HelmValuesFile = HelmValuesFile(sizing=self.sizing, modules_config=self.modules,
                                                     use_ordered_dict=False)
        values_files_cmd_line = self.all_values_files(orig_values_file, values_file)
        values_files_cmd_line = self.add_multiplied_resources(values_files_cmd_line=values_files_cmd_line,
                                                              values_file=values_file)
        self.prepare_helm_charts()
        manifest = self._build_deployment(dry_run=dry_run, command='install',
                                          values_files=values_files_cmd_line, helm_app_name=self.namespace,
                                          helm_ns=self.namespace)
        if dry_run:
            self.save_analyze_manifest(manifest=manifest)

    def all_values_files(self, orig_values_file, values_file):
        full_portal_values = ['--values', str(self.properties.full_portal_values)]
        ini_values = str(values_file.ini_yaml(folder=self.artifacts_time_stamp_folder, properties=self.properties))
        values_files_cmd_line = full_portal_values + ['--values', str(orig_values_file)] + \
                                ['--values', ini_values]
        return values_files_cmd_line

    def prepare_helm_charts(self):
        helm_charts_repo = git_repo.GitRepo(repo=self.properties.helm_charts_repo)
        helm_charts_repo.prepare_git_repo(branch=self.branch, patches=[])
        helm_charts_repo.add_notes(notes=self.properties.get_runtime_property(HELM_APPLICATION_NOTES_KEY))
        run_cmd(cmd=helm_commands.update_dependency())

    def deploy_app(self, properties: TestEnvProperties):
        pass


app = typer.Typer()


@app.command()
def delete(test_env: str = typer.Option('portal', "-e", "--test-env", help="Test environment"),
           namespace: str = typer.Option(..., "-n", "--namespace", help="namespace (application name"),
           del_pvc: bool = typer.Option(False, help="Delete PVC or not")):
    properties = TestEnvProperties(test_env=test_env)
    redeploy = RedeployPortal(test_env=test_env, sizing=None, branch='not_needed', modules=None,
                              multiply_resources=None)
    redeploy.delete_app_pvc(properties=properties, namespace=namespace, del_pvc=del_pvc)


@app.command()
def install(test_env: str = typer.Option('paas_common', "-e", "--test-env", help="Test environment"),
            branch: str = typer.Option(..., "--branch", "-b", help="Helm charts repo branch"),
            sizing: str = typer.Option(..., "--sizing", "-s",
                                       help="Sizing according to ini config files in sizing folder"),
            dry_run: bool = typer.Option(True, help="Helm dry-run option"),
            multiply_resources: str = typer.Option(None, '-m', help="tuple of multiplied resources")):
    """Install Portal deployment """
    if multiply_resources:
        multiply_resources = tuple(multiply_resources.split(','))
    redeploy = RedeployPortal(test_env=test_env, sizing=sizing, branch=branch, modules='basic',
                              multiply_resources=multiply_resources)
    notes = f"deploymentInfo: test_env={test_env},branch={branch},sizing={sizing}"
    redeploy.properties.add_runtime_property(HELM_APPLICATION_NOTES_KEY, notes)
    redeploy.portal_install(dry_run=dry_run)


@app.command()
def upgrade(test_env: str = typer.Option('paas_common', "-e", "--test-env", help="Test environment"),
            branch: str = typer.Option(..., "--branch", "-b", help="Helm charts repo branch"),
            sizing: str = typer.Option(..., "--sizing", "-s",
                                       help="Sizing according to ini config files in sizing folder"),
            dry_run: bool = typer.Option(True, help="Helm dry-run option"),
            get_values: bool = typer.Option(True, help="Get values from running env"),
            multiply_resources: str = typer.Option(None, '-m', help="tuple of multiplied resources")):
    """Upgrade existing Portal deployment """
    if multiply_resources:
        multiply_resources = tuple(multiply_resources.split(','))
    redeploy = RedeployPortal(test_env=test_env, sizing=sizing, branch=branch, modules='basic',
                              multiply_resources=multiply_resources)
    notes = f"deploymentInfo: test_env={test_env},branch={branch},sizing={sizing}"
    redeploy.properties.add_runtime_property(HELM_APPLICATION_NOTES_KEY, notes)
    redeploy.portal_upgrade(dry_run=dry_run, get_values=get_values)


@app.command()
def backup_minio(test_env: str = typer.Option('...', "-e", "--test-env", help="Test environment")):
    redeploy = RedeployPortal(test_env=test_env, sizing=None, branch=None, modules=None,
                              multiply_resources=None)
    folder = Path(redeploy.properties.helm_perf_values_dir, redeploy.namespace)
    minio_folder: Path = Path(folder, 'minio', time_stamp())
    os.makedirs(minio_folder, exist_ok=True)
    redeploy.backup_minio(redeploy.properties, folder=minio_folder)


@app.command()
def restore_minio(test_env: str = typer.Option('...', "-e", "--test-env", help="Test environment"),
                  folder: Path = typer.Option(..., "-f", "--folder",
                                              help="folder with buckets to restore"),
                  buckets: str = typer.Option(MINIO_BUCKETS, "-b", "--buckets",
                                              help="comma separated list of buckets to restore")):
    redeploy = RedeployPortal(test_env=test_env, sizing=None, branch=None, modules=None,
                              multiply_resources=None)
    buckets_list: List[str] = buckets.split(',') if buckets else MINIO_BUCKETS
    redeploy.restore_minio(redeploy.properties, folder=folder, buckets=buckets_list)


if __name__ == "__main__":
    app()
