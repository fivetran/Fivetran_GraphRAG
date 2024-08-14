import boto3

PROFILE = 'datasharing'
session = boto3.Session(profile_name=PROFILE)
athena_client = session.client('athena')

database_name = 'github'
table_name = '_fivetran_relations'
s3_output = 's3://asimov-datalake-s3/path/to/output/'
s3_table_location = 's3://asimov-datalake-s3/github/_fivetran_relations/'

create_table_query = f"""
CREATE TABLE {database_name}.{table_name} (
    head STRING,
    relation STRING,
    tail STRING
)
LOCATION '{s3_table_location}'
TBLPROPERTIES ('table_type'='ICEBERG');
"""

response = athena_client.start_query_execution(
    QueryString=create_table_query,
    ResultConfiguration={
        'OutputLocation': s3_output,
    },
    QueryExecutionContext={
        'Database': database_name
    }
)

query_execution_id = response['QueryExecutionId']
query_execution_status = athena_client.get_query_execution(QueryExecutionId=query_execution_id)
print(query_execution_status)

insert_data_query = f"""
INSERT INTO {database_name}.{table_name} (head, relation, tail)
VALUES
    ('issue', 'has', 'issue_comment'),
    ('user', 'created', 'issue'),
    ('user', 'created', 'issue_comment');
"""

response = athena_client.start_query_execution(
    QueryString=insert_data_query,
    ResultConfiguration={
        'OutputLocation': s3_output,
    },
    QueryExecutionContext={
        'Database': database_name
    }
)

query_execution_id = response['QueryExecutionId']
query_execution_status = athena_client.get_query_execution(QueryExecutionId=query_execution_id)
print(query_execution_status)

print(f"Iceberg table '{table_name}' created and data inserted successfully.")