import os
import time
from pathlib import Path
from typing import List, Optional

import typer
from loguru import logger

from cmdline.common import time_stamp, list_files
from cmdline.configuration import TestEnvProperties, COMMON_PROPERTIES
from cmdline.git import git_repo
from cmdline.helm import helm_commands
from cmdline.helm.helm_commands import HELM_APP_NAME, HELM_NS
from cmdline.helm.helm_values_analysis import HelmValuesAnalysis
from cmdline.helm.values_file import HelmValuesFile, Component
from cmdline.psql_utils import PsqlUtils
from cmdline.redeploy import Redeploy, Step
from cmdline.run_cmd import run_cmd

app = typer.Typer()

DEFAULT_MODULES = "basic"
HELM_APPLICATION_NOTES_KEY = "HELM_APPLICATION_NOTES"
APPLY_PATCH = 'APPLY_PATCH'
NECESSARY_PATCHES = ['elasticsearch.patch', 'postgres-exporter.patch']

# expected format of fromDate argument
FROM_DATE_TIME_FORMAT = "%Y-%m-%dT%H:%M:%S%z"


def apply_patches() -> bool:
    """ global switch - decides if patches are applied to helm charts"""
    apply_patch = os.getenv(APPLY_PATCH)
    if apply_patch is not None and apply_patch.lower() == 'true':
        return True
    return False


PERF_MANIFEST_FILE_NAME = 'manifest.yaml'


class RedeployHelm(Redeploy):
    IMAGE_SOURCE_REPO = 'repo'
    IMAGE_SOURCE_PROPERTIES = 'properties'
    PERF_VALUES_FILE_NAME_SUFFIX = 'values.yaml'

    def __init__(self, branch: Optional[str], sizing: Optional[str], modules: Optional[str],
                 image_source: Optional[str] = IMAGE_SOURCE_REPO):
        super().__init__()
        self.redeploy_steps: List[Step] = self.redeploy_steps + [
            Step(name='upgrade', action=self.upgrade_app, help="upgrade helm deployment")
        ]
        self.image_source: str = image_source
        self.sizing = sizing
        self.modules = modules
        self.branch = branch
        # branch is used as folder and also looked as str in Path, someone is used to name branch as name/xxxx
        self.branch_folder = self.branch.replace('/', '_') if self.branch else None

    def delete_app(self, properties: TestEnvProperties, helm_app_name: Optional[str] = None,
                   namespace: Optional[str] = None):
        if helm_app_name and namespace:
            # use provided not nan values
            run_cmd(helm_commands.uninstall(helm_app_name=helm_app_name, helm_ns=namespace))
        else:
            # use defaults
            run_cmd(helm_commands.uninstall())

    def delete_p_v(self, properties: TestEnvProperties, namespace: str = HELM_NS):
        cmd = ['kubectl', 'delete', '--all', 'pvc', '-n', namespace]
        run_cmd(cmd)

    @staticmethod
    def _build_deployment(values_files: List[str], dry_run: bool = True,
                          upgrade: bool = False,
                          helm_app_name: str = HELM_APP_NAME,
                          helm_ns: str = HELM_NS) -> str:
        cmd_base = helm_commands.install(helm_app_name=helm_app_name, helm_ns=helm_ns, dry_run=dry_run)
        if upgrade:
            cmd_base = helm_commands.upgrade(helm_app_name=helm_app_name, helm_ns=helm_ns, dry_run=dry_run)
        cmd = cmd_base + values_files
        std_out, std_err = run_cmd(cmd)
        return std_out

    def build_deployment(self, properties: TestEnvProperties, dry_run: bool = True, upgrade: bool = False):
        """
        Prepares sizing and helm charts git repositories,
        Call helm to build deployment
        env variable APPLY_PATCH=true can be used to turn on helm charts patches
        """
        # branch is used as folder and also looked as str in Path, someone is used to name branch as name/xxxx
        branch_folder = self.branch.replace('/', '_')
        helm_perf_branch_path: Path = Path(properties.helm_perf_values_dir, branch_folder)
        time_stamp_folder: str = time_stamp()
        helm_artefact_dir = Path(helm_perf_branch_path, properties.test_env)
        save_artefacts_folder: Path = Path(helm_artefact_dir, time_stamp_folder)
        os.makedirs(save_artefacts_folder, exist_ok=True)
        cpt_sizing_repo = git_repo.GitRepo(repo=properties.cpt_sizing_repo)
        cpt_sizing_repo.prepare_git_repo(branch=self.branch)
        perf_values_file_path = Path(helm_artefact_dir,
                                     f"{self.sizing}_{self.modules}_{self.PERF_VALUES_FILE_NAME_SUFFIX}")
        values_files_cmd_line: List[str] = self.set_sizing(properties, perf_values_file=perf_values_file_path)
        all_patches: List[Path] = list_files(folder=properties.patch_folder, ends_with=".patch")
        necessary_patches = [patch for patch in all_patches if patch.parts[-1] in NECESSARY_PATCHES]
        patches: List[Path] = all_patches if apply_patches() else necessary_patches
        helm_charts_repo = git_repo.GitRepo(repo=properties.helm_charts_repo)
        helm_charts_repo.prepare_git_repo(branch=self.branch, patches=patches)
        helm_charts_repo.add_notes(notes=properties.get_runtime_property(HELM_APPLICATION_NOTES_KEY))
        # update dependencies
        run_cmd(cmd=helm_commands.update_dependency())
        manifest = self._build_deployment(dry_run=dry_run, upgrade=upgrade, values_files=values_files_cmd_line)
        if dry_run:
            manifest_file_path = Path(helm_artefact_dir, PERF_MANIFEST_FILE_NAME)
            with open(manifest_file_path, "w", encoding='utf-8') as manifest_file:
                manifest_file.writelines(manifest)
            typer.echo(f"manifest saved to {manifest_file_path}")
            self.save_sizing(folder=save_artefacts_folder, filename=PERF_MANIFEST_FILE_NAME)

    def save_sizing(self, folder: Path, filename: str):
        """
        Create reports from manifest
        :param folder: folder with manifest
        :param filename: filename of manifest
        :return:
        """
        from cmdline.helm.sizing import Sizing
        sizing = Sizing(manifest_file=Path(folder, filename))
        os.makedirs(folder, exist_ok=True)
        typer.echo(f"Save '{self.sizing}' sizing to {folder.resolve()}")
        sizing.save_sizing(save_path=folder, base_file_name=f'{self.sizing}_sizing')

    def deploy_app(self, properties: TestEnvProperties):
        self.build_deployment(properties, dry_run=False)

    def upgrade_app(self, properties: TestEnvProperties):
        self.build_deployment(properties, dry_run=False, upgrade=True)

    def build_deploy(self, properties: TestEnvProperties):
        self.build_deployment(properties, dry_run=False)

    def set_sizing(self, properties: TestEnvProperties, perf_values_file: Path) -> List[str]:
        """
        Creates values yaml file used for building manifest
        :param properties: basic configuration properties
        :param perf_values_file: output file with all values set, used as last helm --values option
        sizing repo when from_template = True
        :return: List of values files helm command line part in form [--values,  str(Paths)]
        """
        values_file: HelmValuesFile = HelmValuesFile(sizing=self.sizing, modules_config=self.modules)
        values_file.merge_property('global/hostname', properties.app_host_name, dst_dict=values_file.values_doc)
        values_file.merge_property('global/db', values_file.global_db(properties), dst_dict=values_file.values_doc)
        values_file.merge_property(key='postgresExporter', value=values_file.postgres_exporter_env(properties),
                                   dst_dict=values_file.values_doc)
        # explicitly set the mmmBe DB name
        values_file.merge_property(f'{Component.MMM_BE.value}/db/dbName', properties.mmm_be_db_name,
                                   dst_dict=values_file.values_doc)
        values_file.set_all_sizing_paths()
        if self.image_source == self.IMAGE_SOURCE_PROPERTIES:
            im_tags = values_file.image_tags(properties.cfg)
            for im_key in im_tags.keys():
                values_file.merge_property(im_key, im_tags[im_key], dst_dict=values_file.values_doc)
        values_file.dump_values_file(dump_file=perf_values_file)
        ondemand_value_file: List[str] = ['--values', str(properties.ondemand_testing_values)]
        template_value_file: List[str] = ['--values', str(properties.perf_values_template)]
        generated_value_file: List[str] = ['--values', str(perf_values_file)]
        # generated_value_file is created from empy
        return ondemand_value_file + template_value_file + generated_value_file

    def delete_p_v_portal(self):
        pass


@app.command()
def sizing_report(sizing: str = typer.Option(..., "--sizing", "-s", help="Sizing according to sizings.ini"),
                  branch: str = typer.Option(..., "--branch", "-b", help="CPT sizing repo branch"),
                  modules: str = typer.Option(..., "--modules", "-m", help="List of enabled modules ")
                  ):
    """Creates values yaml file from given sizing + resource html report"""
    cpt_sizing_repo = git_repo.GitRepo(repo=COMMON_PROPERTIES.cpt_sizing_repo)
    cpt_sizing_repo.prepare_git_repo(branch=branch)
    # perf_init_values_file = None initializes yaml doc to ordered_list with empty ([]) array
    values_file: HelmValuesFile = HelmValuesFile(sizing=sizing, modules_config=modules)
    report_folder = Path(COMMON_PROPERTIES.helm_perf_values_dir, 'generated', branch, sizing, time_stamp())
    typer.echo(f"Report : {report_folder}")
    os.makedirs(report_folder, exist_ok=True)
    values_file.set_all_sizing_paths()
    dump_yaml_file: Path = Path(report_folder, f'{sizing}-values.yaml')
    values_file.dump_values_file(dump_file=dump_yaml_file)
    hva = HelmValuesAnalysis(values_files_folder=report_folder)
    hva.analyze_value_files(branch=branch, report_folder=report_folder)


@app.command()
def analyze_helm_repo(branch: str = typer.Option(..., "--branch", "-b", help="Helm repo branch")):
    """Resources from all values files in given branch"""
    repo = git_repo.GitRepo(repo=COMMON_PROPERTIES.helm_charts_repo)
    repo.prepare_git_repo(branch=branch)
    report_folder = Path(COMMON_PROPERTIES.helm_perf_values_dir, 'charts', branch, "resources")
    hva = HelmValuesAnalysis(values_files_folder=COMMON_PROPERTIES.helm_charts_repo)
    msg = f"Report : {report_folder}"
    logger.info(msg)
    hva.analyze_value_files(branch=branch, report_folder=report_folder)


@app.command()
def steps():
    """Show available steps"""
    RedeployHelm(branch="", sizing="", modules="").show_steps()


@app.command()
def dpe_disk_space(dpe_pod: str = typer.Option(..., "--pod", "-p", help="DPE pod name"),
                   count: int = typer.Option(300, help="number of samples"),
                   wait_sec: int = typer.Option(60, help="sampling period in sec")):
    cmd = ['kubectl', '-n', 'product', 'exec', dpe_pod, '--', 'df', '-h']
    for i in range(count):
        typer.echo(f"{i} : {time_stamp(FROM_DATE_TIME_FORMAT)}")
        std_out, std_err = run_cmd(cmd=cmd)
        typer.echo(std_out)
        time.sleep(wait_sec)


@app.command()
def mmm_db_metrics(test_env: str = typer.Option(..., "--env", "-e", help="Test environment")):
    psql_utils = PsqlUtils(test_env=test_env)
    psql_utils.mmm_metrics()


@app.command()
def pg_dump(test_env: str = typer.Option('paas_common', "--env", "-e", help="Test environment"),
            db: str = typer.Option(None, help="DB name to backup, if None, mmm_be_db_name is used"),
            namespace: str = typer.Option(None, "-n", help='Portal namespace'),
            username: str = typer.Option(None, help='DB username, if None, app_db_user is used'),
            password: str = typer.Option(None, help='DB password, if None, app_db_password is used'),
            dump_type: str = typer.Option(None, help='None, data, schema, all')):
    """pg_dump of mmm_be_db_name"""
    psql_utils = PsqlUtils(test_env=test_env)
    namespace = namespace if namespace else psql_utils.properties.app_namespace
    username = username if username else psql_utils.properties.app_db_user
    password = password if password else psql_utils.properties.app_db_password
    db = db if db else psql_utils.properties.mmm_be_db_name
    psql_utils.pg_dump(db=db, namespace=namespace, username=username, password=password, dump_type=dump_type)


@app.command()
def pg_restore(test_env: str = typer.Option(..., "--env", "-e", help="Test environment"),
               db: str = typer.Option(None, help="DB name to restore, if None, mmm_be_db_name is used"),
               dump_file: Path = typer.Option(..., help="Path to gz dump file", file_okay=True),
               username: str = typer.Option(None, help='DB username. if None, app_db_user is used'),
               password: str = typer.Option(None, help='DB password, if None, app_db_password is used')):
    """
    restore mmm db from dump file

    for creds get original values and sea `userName:`
    or
    user_{db}__{namespace}
    mmmBeDatasourcePassword/ or SPRING_DATASOURCE_PASSWORD
    if not set username and password properties of env are used
    """
    psql_utils = PsqlUtils(test_env=test_env)
    psql_utils.pg_restore(db=db, username=username, password=password, dump_file=dump_file)


@app.command()
def clean_db(test_env: str = typer.Option(..., "--env", "-e", help="Test environment")):
    psql_utils = PsqlUtils(test_env=test_env)
    psql_utils.clean_db()


@app.command()
def redeploy(test_env: str = typer.Option(..., "--env", "-e", help="Test environment"),
             step: str = typer.Option(..., help="run command 'steps' to list available steps"),
             branch: str = typer.Option(..., "--branch", "-b", help="Helm charts repo branch"),
             sizing: str = typer.Option(..., "--sizing", "-s",
                                        help="Sizing according to ini config files in sizing folder"),
             modules: str = typer.Option(DEFAULT_MODULES, "--modules", "-m",
                                         help="List of enabled modules"),
             image_source: str = typer.Option(default=RedeployHelm.IMAGE_SOURCE_REPO,
                                              help=f"Source of component images tags ({RedeployHelm.IMAGE_SOURCE_REPO}|"
                                                   f"{RedeployHelm.IMAGE_SOURCE_PROPERTIES})")):
    """Wraps all redeploy steps for Helm"""
    redeploy_helm = RedeployHelm(image_source=image_source, sizing=sizing, branch=branch, modules=modules)
    filtered_steps: List[Step] = [x for x in redeploy_helm.redeploy_steps if x.name == step]
    if filtered_steps and len(filtered_steps) == 1:
        properties = TestEnvProperties(test_env=test_env)
        notes = f"deploymentInfo: test_env={test_env},branch={branch},sizing={sizing},modules={modules}"
        properties.add_runtime_property(HELM_APPLICATION_NOTES_KEY, notes)
        msg_1 = f"{'=' * 6}\napplication: {helm_commands.HELM_APP_NAME}\n{'=' * 6}\n"
        msg = f"{msg_1}:  branch:{branch},sizing:{sizing},modules:{modules}"
        logger.info(msg)
        redeploy_helm.safe_net(properties=properties)
        s = filtered_steps[0]
        s.action(properties)
    else:
        typer.echo(message=f"ERROR Unknown step {step}", err=True)


if __name__ == "__main__":
    app()
