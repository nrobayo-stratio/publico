from airflow.hooks.dbapi_hook import DbApiHook
from contextlib import closing
from sqlalchemy import create_engine
import urllib
import subprocess

### HOOK ODBC

class SqlServerHook(DbApiHook):
    conn_name_attr = "conn_id"
    default_conn_name = "sqlserver_default"
    supports_autocommit = False
    trusted_connection = "yes"
    server_mssql = "MSSQLSvc/ecbpprq59.uio.bpichincha.com:11459@UIO.BPICHINCHA.COM"

    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.schema = kwargs.pop("schema", None)
        self.driver = kwargs.pop("driver", "{ODBC Driver 18 for SQL Server}")

    def update_kerberos(self):
        conn = self.get_connection(getattr(self, self.conn_name_attr))
        kservice = conn.extra_dejson.get("service", self.server_mssql)
        username=conn.login
        password=conn.password
        cmd = ['kinit', '-S', kservice, username]
        success = subprocess.run(cmd, input=password.encode(), stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL).returncode
        print('kerberos login was: ' + str(success))
        return success

    def get_uri(self):
        conn = self.get_connection(getattr(self, self.conn_name_attr))
        self.update_kerberos()
        trusted_connection = conn.extra_dejson.get("trusted_connection", True)

        conn_string = "DRIVER={driver};SERVER={server};DATABASE={database};".format(
            driver=self.driver,
            server=conn.host,
            database=self.schema or conn.schema,
        )

        conn_string += (
            "Trusted_Connection=yes"
            if trusted_connection
            else "UID={username};PWD={password}".format(
                username=conn.login, password=conn.password or ""
            )
        )

        return "mssql+pyodbc:///?odbc_connect={parameters}".format(
            parameters=urllib.parse.quote_plus(conn_string)
        )

    def get_sqlalchemy_engine(self, engine_kwargs=dict(fast_executemany=True)):
        if engine_kwargs is None:
            engine_kwargs = {}
        return create_engine(self.get_uri(), **engine_kwargs)

    def get_conn(self):
        engine = self.get_sqlalchemy_engine()
        return engine.raw_connection()
    

#Data Models
from enum import Enum
from dataclasses import dataclass


class LoggingLevel(Enum):
    none: int = 0
    basic: int = 1
    performance: int = 2
    verbose: int = 3
    runtime_lineage: int = 4
    custom: int = 100


class ParameterType(Enum):
    PROJECT: int = 20
    PACKAGE: int = 30
    LOGGING: int = 50


@dataclass
class QueryParameters:
    name: str
    value: str
    type: ParameterType

##
#Operator

from typing import Optional, List
from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults
#from airflow.providers.microsoft.mssql.hooks.mssql import MsSqlHook


class SsisPackageOperator(BaseOperator):
    sql_query = """
    DECLARE @execution_id BIGINT
    {reference_query}
    EXEC [SSISDB].[catalog].[create_execution] 
        @folder_name = N'{folder}'
        ,@project_name = N'{project}'
        ,@package_name = N'{package}'
        ,@use32bitruntime = False {reference_parameter}
        ,@execution_id = @execution_id OUTPUT;
    
    {sql_parameters}
    
    DECLARE @LoggingLevel sql_variant = {logging_level}  
    EXEC [SSISDB].[catalog].[set_execution_parameter_value] @execution_id, @object_type=50, @parameter_name=N'LOGGING_LEVEL', @parameter_value=@LoggingLevel;
       
    EXEC [SSISDB].[catalog].[start_execution] @execution_id;
    SELECT @execution_id
    """

    sql_query_parameter = """
    DECLARE @{parameter_name} sql_variant = {parameter_value}
    EXEC [SSISDB].[catalog].[set_execution_parameter_value] @execution_id, @object_type={parameter_type}, @parameter_name=N'{parameter_name}', @parameter_value=@{parameter_name}
"""

    sql_query_reference = """DECLARE @reference_id BIGINT = (SELECT er.[reference_id]
            FROM [SSISDB].[catalog].[environment_references] er
            LEFT JOIN [SSISDB].[catalog].[projects] p ON er.project_id = p.project_id
            LEFT JOIN [SSISDB].[catalog].[folders] f ON p.folder_id = f.folder_id
            WHERE f.name = N'{folder}'
                AND p.name = N'{project}'
                AND er.environment_name = N'{environment}')"""

    sql_reference_parameter = ""

    @apply_defaults
    def __init__(
            self,
            conn_id,
            database: str,
            folder: str,
            project: str,
            package: str,
            environment: Optional[str] = None,
            logging_level: LoggingLevel = LoggingLevel.basic,
            parameters: Optional[List[QueryParameters]] = None,
            *args,
            **kwargs
    ):
        super(SsisPackageOperator, self).__init__(*args, **kwargs)
        self.conn_id = conn_id
        self.database = database
        self.folder = folder
        self.project = project
        self.package = package
        self.environment = environment
        self.sql_parameters = ''
        self.sql_reference_query = ''
        self.logging_level = logging_level
        if parameters:
            self.__build_query_parameters(parameters=parameters)
        if environment:
            self.__build_query_reference()
            self.sql_reference_parameter = f"\n{' '*8},@reference_id = @reference_id"
        self.__build_sql_query()

    def __build_query_parameters(self, parameters: list[QueryParameters]):
        for parameter in parameters:
            self.sql_parameters += SsisPackageOperator.sql_query_parameter.format(
                parameter_name=parameter.name.replace("'", "''"),
                parameter_value=parameter.value.replace("'", "''"),
                parameter_type=parameter.type.value
            )

    def __build_query_reference(self):
        self.sql_reference_query = SsisPackageOperator.sql_query_reference.format(
            folder=self.folder,
            project=self.project,
            environment=self.environment
        )

    def __build_sql_query(self):
        self.sql = SsisPackageOperator.sql_query.format(
            folder=self.folder,
            project=self.project,
            package=self.package,
            environment=self.environment,
            reference_query=self.sql_reference_query,
            reference_parameter=self.sql_reference_parameter,
            sql_parameters=self.sql_parameters,
            logging_level=self.logging_level.value
        )

    def execute(self, context):
        sqlserver_hook = SqlServerHook(
            conn_id=self.conn_id,
            schema=self.database
        )

        self.log.info(f"Running package using SQL: \n {self.sql}")

        result = sqlserver_hook.get_first(self.sql)

        if not result or len(result) < 1:
            self.log.info(result)
            raise ValueError("No execution ID was returned")

        self.xcom_push(
            context=context,
            key="execution_id",
            value=result[0]
        )
    

    @classmethod
    def build_parameters(cls, json_parameters):
        parameters = []
        for parameter in json_parameters:
            parameters.append(QueryParameters(name=parameter["name"],
                                              value=parameter["value"],
                                              type=ParameterType(parameter["type"])))
        parameters = parameters if len(parameters)>0 else None
        return parameters

#End of Custom operator
#####

from airflow.models.dag import DAG
from airflow.models import Variable
from airflow.operators.bash import BashOperator
from datetime import datetime, timedelta


with DAG(
    "ssisodbc_test_dag",
    default_args={"retries": 2},
    description="DAG test for SSIS using odbc",
    schedule=None,
    start_date=datetime(2021, 1, 1),
    catchup=False,
    tags=["example"],
) as dag:

    
    t1 = BashOperator(
        task_id="print_date",
        bash_command="date",
    )

    t2 = BashOperator(
        task_id="freeze",
        depends_on_past=False,
        bash_command="pip freeze",
        retries=3,
    )

    t3 = SsisPackageOperator(
        task_id="SSIS",
        conn_id=Variable.get("SSIS_ODBC_CONFIG", deserialize_json=True).get("conn_id"),
        database=Variable.get("SSIS_ODBC_CONFIG", deserialize_json=True).get("database"),
        folder=Variable.get("SSIS_ODBC_CONFIG", deserialize_json=True).get("folder"),
        project=Variable.get("SSIS_ODBC_CONFIG", deserialize_json=True).get("project"),
        package=Variable.get("SSIS_ODBC_CONFIG", deserialize_json=True).get("package"),
        parameters=SsisPackageOperator.build_parameters(Variable.get("SSIS_CONFIG", deserialize_json=True).get("parameters", []))
    )

    t1 >> [t2, t3]