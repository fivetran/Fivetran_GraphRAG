from whyhow import WhyHow
import boto3
from pyiceberg.catalog import load_catalog
from whyhow.schemas import Schema, SchemaEntity, SchemaEntityField, SchemaRelation, SchemaTriplePattern
import os
import random
import csv

REGION = 'us-west-2'
PROFILE = 'datasharing'
NAMESPACE = 'github'

underlying_db = load_catalog(
    "glue",
    **{"type": "glue", "s3.region": REGION, "profile_name": PROFILE},
)

def get_glue_tables(database_name):
    session = boto3.Session(profile_name=PROFILE)
    glue_client = session.client('glue')

    tables = []
    paginator = glue_client.get_paginator('get_tables')

    for page in paginator.paginate(DatabaseName=database_name):
        tables.extend(page['TableList'])

    return tables

output_dir = "github_data"
os.makedirs(output_dir, exist_ok=True)

entities = {}
tables = get_glue_tables(NAMESPACE)
for table in tables:
    table_name = table['Name']
    if table_name == "_fivetran_relations":
        continue

    entity_table = underlying_db.load_table(f"{NAMESPACE}.{table_name}")
    df = entity_table.scan().to_pandas()

    csv_path = os.path.join(output_dir, f"{table_name}.csv")
    df.to_csv(csv_path, index=False)

    fields = [SchemaEntityField(name=col) for col in df.columns]
    entity = SchemaEntity(name=table_name, description="", fields=fields)
    entities[table_name] = entity

relation_table = underlying_db.load_table(f"{NAMESPACE}._fivetran_relations")
df = relation_table.scan().to_pandas()
relations = {relation: SchemaRelation(name=relation, description='') for relation in df['relation'].unique()}
patterns = {SchemaTriplePattern(head=entities[row['head']], relation=relations[row['relation']], tail=entities[row['tail']], description='') for _, row in df.iterrows()}

client = WhyHow(api_key="key", base_url="https://api.whyhow.ai")

random_number = random.randint(100, 999)
workspace = client.workspaces.create(f"github_example_{random_number}")

schema = client.schemas.create(
    workspace_id=workspace.workspace_id,
    name="github",
    entities=entities,
    relations=relations,
    patterns=patterns,
)

for file in os.listdir(output_dir):
    if file.endswith(".csv"):
        document_path = os.path.join(output_dir, file)
        document = client.documents.upload(
            document_path,
            workspace_id=workspace.workspace_id
        )
        print(f"Document {file} Added:", document)