import io
import json
from pathlib import Path
from typing import List, Union, Optional

import pandas as pd
import typer
from loguru import logger

import os
from cmdline.common import archive_folder
from cmdline.configuration import TestEnvProperties, COMMON_PROPERTIES
from cmdline.constants import CPT_HOME
from cmdline.run_cmd import RunCommand


class PsqlUtils:
    def __init__(self, test_env: Union[str, None], properties: TestEnvProperties = None):
        if test_env is None and properties is None:
            msg = "Both test env and properties are None"
            logger.error(msg)
            raise ValueError(msg)
        self.properties: TestEnvProperties = TestEnvProperties(test_env=test_env) if test_env else properties

    def psql_options(self, pg_database: str = 'postgres') -> List[str]:
        pg_port = self.properties.app_db_port
        pg_host = self.properties.app_db_host_name
        pg_user = self.properties.app_db_user
        pg_role = self.properties.app_db_role
        self.set_pg_env_vars(username=pg_user, password=self.properties.app_db_password)
        db_options = ['-d', pg_database]
        pg_options = ['-h', pg_host, '-p', pg_port, '-U', pg_user]
        return db_options + pg_options

    def set_pg_env_vars(self, username, password):
        os.putenv('PGUSER', username)
        os.putenv('PGPASSWORD', password)
        os.putenv('PGHOST', self.properties.app_db_host_name)

    def psql_cmd(self, options: List[str], sql_folder: str = 'sql', sql_file: str = None):
        cmd = ['psql', '--csv'] + options
        sql_path = Path(CPT_HOME, sql_folder).resolve()
        if sql_file:
            file_option = ['-f', str(Path(sql_path, sql_file).resolve())]
            cmd = cmd + file_option
        logger.info(f"Run psql {cmd}")
        run_cmd: RunCommand = RunCommand(command=cmd)
        std_msg = f'\npsql_cmd stdout\n====\n{run_cmd.stdout_decoded}\n===='
        logger.info(std_msg)
        if run_cmd.return_code != 0:
            msg = run_cmd.error_msg()
            logger.error(msg)
            return None
        return run_cmd.stdout_decoded

    def mmm_metrics(self):
        # default value of pg_database=postgres
        opt = self.psql_options(pg_database=self.properties.mmm_be_db_name)
        csv_out = self.psql_cmd(opt, sql_file='mmm_db_metrics.sql')
        if isinstance(csv_out, str):
            # noinspection PyTypeChecker
            mmm_db_df = pd.read_csv(io.StringIO(csv_out))
            keys = mmm_db_df['metrics_name']
            # Rounded Down
            counts = mmm_db_df['count'].values.astype(int)
            # convert most NumPy values to a native Python type:to be json serializable
            values = [val.item() for val in counts]
            dictionary = dict(zip(keys, values))
            json_metrics = json.dumps(dictionary, indent=3)
            typer.echo(json_metrics)
            return json_metrics
        return None

    def clean_db(self):
        # default value of pg_database=postgres
        opt = self.psql_options()
        self.psql_cmd(opt, sql_folder='sql/postgresql', sql_file='clean_dbs.sql')

    def set_jdbc_string(self):
        logger.info(f"{self.properties.test_env}:Restore DB dump")

    def pg_restore(self, dump_file: Path, db: Optional[str], username: Optional[str], password: Optional[str]):
        import tempfile
        import multiprocessing
        temp_dir = tempfile.TemporaryDirectory()
        try:
            logger.info(f"{self.properties.test_env}: Restore DB dump from {dump_file}")
            logger.info(f"Extract tar {dump_file} to {temp_dir.name}")
            cmd = ['tar', '--strip-components=1', '-C', temp_dir.name, '-xvzf', str(dump_file)]
            run_cmd: RunCommand = RunCommand(command=cmd)
            typer.echo(run_cmd.error_msg())
            num_cpu = multiprocessing.cpu_count()
            if not db:
                db = self.properties.mmm_be_db_name
                typer.echo(f'Use db {db} from properties of test env {self.properties.test_env}')
            restore_options = ['--clean', '--if-exists', '--no-owner', '-j', str(num_cpu), '-d', db, str(temp_dir.name)]
            if username and password:
                #  for portal eliminate copy / paste mistake
                assert db in username
                # sets user and password from cmd line arg
                self.set_pg_env_vars(username=username, password=password)
            else:
                # sets env vars user and password from props, option not used
                opt = self.psql_options(pg_database=db)
            logger.info(f'Restore options {restore_options}')
            cmd = ['pg_restore'] + restore_options
            typer.echo(f'pg dump command: {cmd} in {os.getcwd()}')
            run_cmd: RunCommand = RunCommand(command=cmd)
            typer.echo(run_cmd.error_msg())
        finally:
            logger.info(f'Cleaning {temp_dir}')
            temp_dir.cleanup()

    def pg_dump(self, db: str, namespace: str, username: str, password: str, dump_type: Optional[str] = None):
        """type = data, schema, None = all"""
        import tempfile
        import multiprocessing
        msg = (f'pg dump of: {db}, namespace: {namespace}, dump type: {dump_type}, '
               f'db host:{self.properties.app_db_host_name}')
        logger.info(msg)
        temp_dir = tempfile.TemporaryDirectory()
        # go back from tmp dir
        current_cwd = os.getcwd()
        try:
            dest_path = Path(COMMON_PROPERTIES.cpt_artefacts_dir, 'pg_dumps', namespace)
            logger.info(f"{self.properties.test_env}: Create DB dump to {temp_dir}")
            dump_dir_name = f"postgresql_{db}"
            if dump_type:
                dump_dir_name = f'{dump_dir_name}_{dump_type}'
            num_cpu = multiprocessing.cpu_count()
            dump_options = ['-j', str(num_cpu), '--format', 'd', '--no-privileges', '--no-owner',
                            '--file', dump_dir_name]
            if dump_type:
                dump_options = dump_options + [f'--{dump_type}-only']
            if username and password:
                # sets user and password from cmd line arg
                self.set_pg_env_vars(username=username, password=password)
            else:
                # sets env vars user and password from props, option not used for pg_dump
                opt = self.psql_options(pg_database=db)
            os.chdir(temp_dir.name)
            typer.echo(f'inside {os.getcwd()} dir')
            cmd = ['pg_dump'] + dump_options + [db]
            typer.echo(f'pg dump command: {cmd} in {os.getcwd()}')
            run_cmd: RunCommand = RunCommand(command=cmd)
            typer.echo(run_cmd.error_msg())
            src_path = Path(temp_dir.name, dump_dir_name).resolve()
            if src_path.exists():
                os.makedirs(dest_path, exist_ok=True)
                archive_folder(src_path=Path(temp_dir.name, dump_dir_name).resolve(), dest_path=dest_path,
                               dest_base_file_name=dump_dir_name)
            else:
                typer.echo(f'{src_path} does not exist')
        finally:
            typer.echo(f'chdir back to {current_cwd} before temp dir "{temp_dir.name}" clean up')
            os.chdir(current_cwd)
            temp_dir.cleanup()

    def dpm_job_info(self):
        opt = self.psql_options(pg_database='dpm')
        std_out = self.psql_cmd(opt, sql_folder='bin/pycpt/sql', sql_file='dpm_job_info.sql')
        typer.echo(std_out)

    def mmm_job_info(self):
        opt = self.psql_options(pg_database='mmm')
        std_out = self.psql_cmd(opt, sql_folder='bin/pycpt/sql', sql_file='mmm_job_info.sql')
        typer.echo(std_out)
