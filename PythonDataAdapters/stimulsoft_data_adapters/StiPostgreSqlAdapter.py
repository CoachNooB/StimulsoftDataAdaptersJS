"""
Stimulsoft.Reports.JS
Version: 2024.3.1
Build date: 2024.06.13
License: https://www.stimulsoft.com/en/licensing/reports
"""

from .classes.StiDataResult import StiDataResult
from .StiDataAdapter import StiDataAdapter


class StiPostgreSqlAdapter(StiDataAdapter):

### Properties

    version = '2024.3.1'
    checkVersion = True


### Methods

    def connect(self):
        if self.connectionInfo.driver:
            return self.connectOdbc()
        
        if not self.connectionInfo.charset:
            self.connectionInfo.charset = 'utf8'
        
        connectionString: str = \
            f"host='{self.connectionInfo.host}' " \
            f"port='{self.connectionInfo.port}' " \
            f"dbname='{self.connectionInfo.database}' " \
            f"user='{self.connectionInfo.userId}' " \
            f"password='{self.connectionInfo.password}' " \
            f"options='--client_encoding={self.connectionInfo.charset}' "

        try:
            import psycopg
            self.connectionLink = psycopg.connect(connectionString)
        except Exception as e:
            return StiDataResult.getError(str(e)).getDataAdapterResult(self)
        
        return StiDataResult.getSuccess().getDataAdapterResult(self)
    
    def process(self):
        if super().process():
            return True

        self.connectionInfo.port = 5432

        parameterNames = {
            'driver': ['driver'],
            'host': ['server', 'host', 'location'],
            'port': ['port'],
            'database': ['database', 'data source', 'dbname'],
            'userId': ['uid', 'user', 'username', 'userid', 'user id'],
            'password': ['pwd', 'password'],
            'charset': ['charset']
        }

        return self.processParameters(parameterNames)
    
    def getType(self, meta: tuple):
        if self.connectionInfo.driver:
            return super().getType(meta)
        
        import psycopg
        column: psycopg.Column = meta
        
        types = {
            'int': ['int', 'int2', 'int4', 'int8', 'smallint', 'bigint', 'tinyint', 'integer', 'numeric', 'uniqueidentifier'],
            'number': ['float', 'float4', 'float8', 'real', 'double', 'decimal', 'smallmoney', 'money'],
            'boolean': ['bool', 'boolean'],
            'datetime': ['abstime', 'time', 'date', 'datetime', 'smalldatetime', 'timestamp'],
            'time': ['timetz', 'timestamptz'],
            'array': ['bytea', 'array']
        }

        for key, array in types.items():
            typeName = psycopg.adapters.types[column.type_code].name
            if typeName in array:
                return key

        return 'string'
    
    def makeQuery(self, procedure: str, parameters: list):
        paramsString = super().makeQuery(procedure, parameters)
        return f'CALL {procedure} ({paramsString})'