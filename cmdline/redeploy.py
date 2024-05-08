import abc
from pathlib import Path
from typing import List, NamedTuple, Callable, Optional

import typer
from loguru import logger

from cmdline.configuration import TestEnvProperties
from cmdline.helm import kubectl_commands
from cmdline.helm.helm_commands import HELM_NS
from cmdline.psql_utils import PsqlUtils
from cmdline.run_cmd import run_cmd

MINIO_BUCKETS = ['lookups', 'components', 'shared']

PATCH_FAILED_ERROR = "patch failed"

# minio executable file
MINIO_CLIENT = 'minio_client'


class Step(NamedTuple):
    action: Callable[[TestEnvProperties], None]
    name: str = None
    help: str = None


class Redeploy(abc.ABC):
    def __init__(self):
        super().__init__()
        self.redeploy_steps: List[Step] = \
            [Step(name='del_app', action=self.delete_app, help="Uninstall deployment of ONE2"),
             Step(name='del_pv', action=self.delete_p_v, help="Delete DPE and ES persistence volumes"),
             Step(name='clean_db', action=self.clean_db, help="Drop (if exists) and create all dbs"),
             Step(name='new_deploy', action=self.new_deploy, help="Delete app, persistent volumes, build and deploy"),
             Step(name='clean_deploy', action=self.clean_deploy,
                  help="Delete app, persistent volumes, DBs, build and deploy"),
             Step(name='build_deploy', action=self.build_deploy,
                  help="Build manifest and install (HELM:update dependency, set values,dry-run=False)"),
             Step(name='build', action=self.build_deployment,
                  help="HELM:update dependency,set values,dry-run=True; "
                       "Kustomize: build manifest, set variables, create sizing report"),
             Step(name='install', action=self.deploy_app,
                  help="HELM: build_deploy (dry-run=False), Kustomize: apply latest manifest"),
             Step(name='clean_minio', action=self.minio_clean,
                  help="Creates minio alias and removes profiling and executor content"),
             Step(name='import_lookups', action=self.minio_import, help="Copy lookups folder from local to minio"),
             Step(name='delete_lookups', action=self.clean_lookups,
                  help="Delete lookups folder and import lookups from local")
             ]

    @abc.abstractmethod
    def delete_app(self, properties: TestEnvProperties, helm_app_name: Optional[str], namespace: Optional[str]):
        raise NotImplementedError

    @abc.abstractmethod
    def delete_p_v(self, properties: TestEnvProperties, namespace: str):
        raise NotImplementedError

    @abc.abstractmethod
    def delete_p_v_portal(self):
        raise NotImplementedError

    @abc.abstractmethod
    def build_deployment(self, properties: TestEnvProperties):
        raise NotImplementedError

    @abc.abstractmethod
    def deploy_app(self, properties: TestEnvProperties):
        raise NotImplementedError

    def clean_db(self, properties: TestEnvProperties):
        psql_utils: PsqlUtils = PsqlUtils(test_env=None, properties=properties)
        logger.info(f"{properties.test_env}: Drop and create all DBs")
        # sql/postgresql/clean_dbs.sql
        # default db is postgres
        opt = psql_utils.psql_options()
        psql_utils.psql_cmd(opt, sql_folder='sql/postgresql', sql_file='clean_dbs.sql')

    def build_deploy(self, properties: TestEnvProperties):
        self.build_deployment(properties)
        self.deploy_app(properties)

    def new_deploy(self, properties: TestEnvProperties, namespace: str = HELM_NS):
        """for portal app name == namespace"""
        self.delete_app(properties, helm_app_name=namespace, namespace=namespace)
        self.delete_p_v(properties, namespace=namespace)
        self.build_deploy(properties)

    def delete_app_pvc(self, properties: TestEnvProperties, namespace: str, del_pvc: bool):
        """for portal app name == namespace"""
        self.delete_app(properties=properties, helm_app_name=namespace, namespace=namespace)
        if del_pvc:
            self.delete_p_v_portal()

    def clean_deploy(self, properties: TestEnvProperties, namespace: str = HELM_NS):
        self.delete_app(properties, helm_app_name=namespace, namespace=namespace)
        self.delete_p_v(properties, namespace=namespace)
        self.clean_db(properties)
        self.build_deploy(properties)

    def minio_clean(self, properties: TestEnvProperties):
        self.minio_alias(properties)
        self.clean_minio(properties)

    def minio_import(self, properties: TestEnvProperties):
        self.minio_alias(properties)
        self.import_lookups(properties)

    def show_steps(self):
        header = f"{10 * '='} steps description {10 * '='}"
        typer.echo(header)
        sorted_steps = sorted(self.redeploy_steps, key=lambda x: x.name)
        for step in sorted_steps:
            typer.echo(f"{step.name}{(20 - len(step.name)) * ' '}{step.help}")
        typer.echo(f"{len(header) * '='}")

    def safe_net(self, properties: TestEnvProperties):
        """ ensure that correct (i.e. equal to test env) k8s context is used """
        test_env: str = properties.test_env
        # kube.context is set as property for given test env
        kube_context = properties.kube_context
        kubectl_commands.use_context(kube_context)
        cc = kubectl_commands.current_context()
        # by convention test env starts by k8s context
        # e.g. paas_dq_sa is serverless aurora in paas_dq
        assert test_env.startswith(cc)
        msg = f"Context OK: {test_env} starts with {cc}"
        logger.info(msg)

    @staticmethod
    def minio_alias(properties: TestEnvProperties):
        """minio executable renamed to `minio_client` - `mc` is conflicting with midnight commander"""
        alias_name = properties.minio_alias_name
        url = properties.minio_url
        access_key = properties.minio_access_key
        secret_key = properties.minio_secret_key
        cmd = [MINIO_CLIENT, 'alias', 'set', alias_name, url, access_key, secret_key]
        run_cmd(cmd=cmd)

    def clean_minio(self, properties: TestEnvProperties):
        logger.info(f"Clean minio in test env :{properties.test_env}")
        alias_name = properties.minio_alias_name
        cmd = [MINIO_CLIENT, 'rm', '--recursive', '--force', f"{alias_name}/profiling"]
        run_cmd(cmd)
        cmd = [MINIO_CLIENT, 'rm', '--recursive', '--force', f"{alias_name}/executor"]
        run_cmd(cmd)

    def import_lookups(self, properties: TestEnvProperties, delete_lookups: bool = False):
        logger.info(f"Import lookups to test env = {properties.test_env}")
        alias_name = properties.minio_alias_name
        lookup_dir: Path = properties.minio_lookup_dir
        if delete_lookups:
            cmd = [MINIO_CLIENT, 'rm', '--recursive', '--force', f"{alias_name}/lookups"]
            run_cmd(cmd=cmd)
        cmd = [MINIO_CLIENT, 'cp', '--recursive', str(lookup_dir), alias_name]
        run_cmd(cmd=cmd)

    def clean_lookups(self, properties: TestEnvProperties):
        self.import_lookups(properties, delete_lookups=True)

    def backup_minio(self, properties: TestEnvProperties, folder: Path = Path.cwd()):
        logger.info(f"Backup {MINIO_BUCKETS} for test env = {properties.test_env} to {folder.resolve()}")
        self.minio_alias(properties)
        alias_name = properties.minio_alias_name
        for bucket in MINIO_BUCKETS:
            cmd = [MINIO_CLIENT, 'cp', '--recursive', f'{alias_name}/{bucket}', str(folder)]
            run_cmd(cmd=cmd)

    def restore_minio(self, properties: TestEnvProperties, folder: Path, buckets: List[str]):
        """restore minio buckets from folder"""
        alias_name = properties.minio_alias_name
        self.minio_alias(properties)
        for bucket in buckets:
            logger.info(f"Restore {bucket} for test env = {properties.test_env} "
                        f"from {folder.resolve()}/{bucket}")
            #  minio.lookup.rel.dir=../lookups -> alias_name bucket lookups must exist
            #  bucket must exist (minio_client mb minio_alias/{bucket}
            bucket_folder: Path = Path(folder, bucket)
            if bucket_folder.is_dir():
                cmd = [MINIO_CLIENT, 'cp', '--recursive', f'{bucket_folder.resolve()}', f'{alias_name}']
                run_cmd(cmd=cmd)
            else:
                raise FileNotFoundError(bucket_folder)

    def __repr__(self):
        return f"{self.__class__.__name__}"

    def __str__(self):
        return repr(self)
