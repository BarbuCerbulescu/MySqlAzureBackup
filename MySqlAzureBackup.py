from azure.data.tables import TableServiceClient, UpdateMode
from azure.core.credentials import AzureNamedKeyCredential
import mysql.connector




def get_azure_params() -> dict:
    azureInputFile=open('azure_input.txt')
    azureParamValues=azureInputFile.read().splitlines()
    if len(azureParamValues)!=3: raise Exception('Azure Input file not configured properly! Make sure the parameters are written on separate lines in the following order: storage account, key, password, endpoint.')
    azureParams={}
    azureParams['storage_account']=azureParamValues[0]
    azureParams['key']=azureParamValues[1]
    azureParams['endpoint']=azureParamValues[2]
    return azureParams

azureParams=get_azure_params()
credential = AzureNamedKeyCredential(azureParams['storage_account'], azureParams['key'])
table_service_client = TableServiceClient(endpoint=azureParams['endpoint'], credential=credential)




def get_mysql_connector_params() -> dict:
    mysqlInputFile = open('mysql_connector_input.txt', 'r')
    mysqlParamValues = mysqlInputFile.read().splitlines()
    mysqlInputFile.close()
    if len(mysqlParamValues) != 4: raise Exception(
        'Mysql Input file not configured properly! Make sure the parameters are written on separate lines in the following order: host, user, password, database.')
    mysqlParams = {}
    mysqlParams['host'] = mysqlParamValues[0]
    mysqlParams['user'] = mysqlParamValues[1]
    mysqlParams['password'] = mysqlParamValues[2]
    mysqlParams['database'] = mysqlParamValues[3]
    return mysqlParams

mysqlParams=get_mysql_connector_params()
mydb = mysql.connector.connect(
host=mysqlParams['host'],
user=mysqlParams['user'],
password=mysqlParams['password'],
database=mysqlParams['database'],
auth_plugin='mysql_native_password')
mycursor = mydb.cursor()








##### Generalized Entity Section ################################################################
def sort_dict_alphabetically(dictionary):
    return sorted(dictionary.items(), key=lambda item: item[0].lower())

class GeneralizedEntity:
    def __init__(self, tableName:str, id, data:dict):
        self._tableName=tableName
        self._id=int(id)
        data=sort_dict_alphabetically(data)
        data=tuple((key,value) for key,value in data)
        self._data=data
    @property
    def tableName(self):
        return self._tableName
    @property
    def id(self):
        return self._id
    @property
    def data(self):
        return self._data
    def __hash__(self):
        return hash((self._id,self._data))
    def __eq__(self, other):
        if self._id==other._id and self._data==other._data: return True
        else: return False
    def __repr__(self):
        return f"(tableName:{self._tableName}, id:{self._id}, data:{self._data})"


def convert_azure_entity_to_GeneralizedEntity(azureEntity) -> GeneralizedEntity:
    tableName=azureEntity.pop("PartitionKey")
    id = azureEntity.pop('RowKey')
    data=dict(zip(azureEntity.keys(),azureEntity.values()))
    return GeneralizedEntity(tableName,id,data)

def convert_dict_to_GeneralizedEntity(dictionary:dict) -> GeneralizedEntity:
    if "id" not in dictionary or "tableName" not in dictionary:
        raise Exception("ERROR:can't convert dictionary to Entity; either no id or no tableName present")
    else:
        data=dictionary
        tableName=data.pop("tableName")
        id=data.pop("id")
        return GeneralizedEntity(tableName, id, data)

def convert_GeneralizedEntity_to_dict_for_azure(entity:GeneralizedEntity) -> dict:
    auxDataList = list(entity.data)
    auxDataList.insert(0, ("PartitionKey", entity.tableName))
    auxDataList.insert(1, ("RowKey", str(entity.id)))
    entityDict = {tuple[0]: tuple[1] for tuple in auxDataList}
    return entityDict

dataTypeConversionFuncDict={b'int': lambda x: int(x), b'varchar': lambda x:str(x), b'double': lambda x:float(x)}
def convert_GeneralizedEntity_to_dict_for_mysql(entity:GeneralizedEntity,dataTypes:tuple) -> dict:
    auxDataTuple = (("id", entity.id),) + entity.data
    noOfColumns=len(auxDataTuple)
    entityDict = {auxDataTuple[i][0]: dataTypeConversionFuncDict[dataTypes[i]](auxDataTuple[i][1]) for i in
                    range(0, noOfColumns)}
    return entityDict
################################################################################################################








###### Table Related Functions Section ############################################################################
def get_mysql_table_names() -> tuple:
    mycursor.execute('SHOW TABLES;')
    tableNames = tuple(tableTuple[0] for tableTuple in mycursor.fetchall())
    return tableNames


def get_mysql_table_data_types_ordered_by_column_names(tableName:str) -> tuple:
    selectDataTypesSortedByColumnNamesQuery = "SELECT sub_query_table.data_type " \
                                              "FROM ( SELECT column_name,data_type " \
                                              "FROM information_schema.columns " \
                                              "WHERE table_schema = %s AND table_name = %s " \
                                              "ORDER BY column_name) AS sub_query_table;"
    mycursor.execute(selectDataTypesSortedByColumnNamesQuery, ('practica_azure', tableName))
    dataTypes = tuple(typeTuple[0] for typeTuple in mycursor.fetchall())
    return dataTypes


def upsert_entity_into_mysql_table(entity:GeneralizedEntity,tableName:str,dataTypes:tuple=None):
    if dataTypes is None: dataTypes=get_mysql_table_data_types_ordered_by_column_names(tableName)
    dataToInsert = convert_GeneralizedEntity_to_dict_for_mysql(entity, dataTypes)
    stringOfColumns = ",".join(dataToInsert.keys())
    tupleOfValues = tuple(dataToInsert.values())
    valuesFormatString = ','.join(['%s'] * len(dataToInsert.values()))
    replaceStatement = f"REPLACE INTO {tableName}({stringOfColumns}) VALUES({valuesFormatString});"
    mycursor.execute(replaceStatement, tupleOfValues)
    mydb.commit()


def delete_entity_from_mysql_table(entity:GeneralizedEntity,tableName:str,dataTypes:tuple=None):
    if dataTypes is None: dataTypes = get_mysql_table_data_types_ordered_by_column_names(tableName)
    dataToDelete = convert_GeneralizedEntity_to_dict_for_mysql(entity, dataTypes)
    sqlWhereClauseList = [f"{column}=%s" for column in dataToDelete.keys()]
    sqlWhereClause = " AND ".join(sqlWhereClauseList)
    tupleOfValues = tuple(dataToDelete.values())
    mycursor.execute(f"DELETE FROM {tableName} WHERE {sqlWhereClause};", tupleOfValues)
    mydb.commit()


def get_set_of_all_mysql_entities_of_table(tableName:str, tableClient=None) -> set :
    setOfAllEntitiesFromMysql=set()
    mycursor.execute(f"SELECT * FROM {tableName}")
    allMysqlEntitiesTuples = mycursor.fetchall()
    for valueTuple in allMysqlEntitiesTuples:
        data = dict(zip(('tableName',) + mycursor.column_names, (tableName,) + valueTuple))
        entity = convert_dict_to_GeneralizedEntity(data)
        setOfAllEntitiesFromMysql.add(entity)
    return  setOfAllEntitiesFromMysql




def get_azure_table_client(tableName:str):
    table_service_client.create_table_if_not_exists(table_name=tableName)
    table_client = table_service_client.get_table_client(table_name=tableName)
    return table_client


def upsert_entity_using_azure_table_client(entity:GeneralizedEntity,tableClient):
    azureEntity = convert_GeneralizedEntity_to_dict_for_azure(entity)
    tableClient.upsert_entity(entity=azureEntity, mode=UpdateMode.REPLACE)


def delete_entity_using_azure_table_client(entity:GeneralizedEntity, tableClient):
    azureEntity = convert_GeneralizedEntity_to_dict_for_azure(entity)
    tableClient.delete_entity(entity=azureEntity)


def get_set_of_all_azure_entities_using_table_client(tableClient) -> set :
    setOfAllEntitiesFromAzure = set()
    azureEntities = tableClient.list_entities()
    for azureEntity in azureEntities:
        entity = convert_azure_entity_to_GeneralizedEntity(azureEntity)
        setOfAllEntitiesFromAzure.add(entity)
    return setOfAllEntitiesFromAzure

#################################################################################################################








def main():
    operationTypeAlias=input("Type 'bup' for backup and 'rec' for recovery: ")
    while operationTypeAlias!='bup' and operationTypeAlias!='rec':
        operationTypeAlias=input("Operation type not recognized! Please type 'bup' for backup and 'rec' for recovery: ")
    if operationTypeAlias=='bup': operationType='backup'
    else : operationType='recovery'

    tableNames = get_mysql_table_names()
    for tableName in tableNames:

        tableClient=get_azure_table_client(tableName)

        setOfAllEntitiesFromAzure = get_set_of_all_azure_entities_using_table_client(tableClient)

        setOfAllEntitiesFromMysql = get_set_of_all_mysql_entities_of_table(tableName)

        if operationType=='backup':
            entitiesToUpsertInAzure=setOfAllEntitiesFromMysql.difference(setOfAllEntitiesFromAzure)
            for entity in entitiesToUpsertInAzure:
                upsert_entity_using_azure_table_client(entity,tableClient)

            entitiesToDeleteInAzure = setOfAllEntitiesFromAzure.difference(setOfAllEntitiesFromMysql)
            for entity in entitiesToDeleteInAzure:
                delete_entity_using_azure_table_client(entity,tableClient)


        else: # if operationType=='recovery'
            dataTypes = get_mysql_table_data_types_ordered_by_column_names(tableName)

            entitiesToUpsertInMysql = setOfAllEntitiesFromAzure.difference(setOfAllEntitiesFromMysql)
            for entity in entitiesToUpsertInMysql:
                upsert_entity_into_mysql_table(entity, tableName, dataTypes)

            entitiesToDeleteInMysql = setOfAllEntitiesFromMysql.difference(setOfAllEntitiesFromAzure)
            for entity in entitiesToDeleteInMysql:
                delete_entity_from_mysql_table(entity, tableName, dataTypes)


if __name__ == '__main__':
    main()
    


