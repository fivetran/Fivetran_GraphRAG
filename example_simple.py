from whyhow import WhyHow
from whyhow.schemas import Schema, SchemaEntity, SchemaEntityField, SchemaRelation, SchemaTriplePattern

client = WhyHow(api_key="d5b8dc9af83d6bad2607d578cb2f69b2", base_url="https://api.whyhow.ai")

print("Creating Workspace!")
workspace = client.workspaces.create("github_example")

print("Created workspace: ", workspace)

for file in ['issue.csv', 'issue_comment.csv', 'user.csv']:
    document_path = f"data/{file}"
    document = client.documents.upload(
        document_path,
        workspace_id=workspace.workspace_id
    )
    print(f"Document {file} Added:", document)


issue_entity = SchemaEntity(name="issue", description="", fields=[
        SchemaEntityField(name='id'),
        SchemaEntityField(name='created_at'),
        SchemaEntityField(name='updated_at'),
        SchemaEntityField(name='number'),
        SchemaEntityField(name='state'),
        SchemaEntityField(name='title'),
        SchemaEntityField(name='body'),
        SchemaEntityField(name='locked'),
        SchemaEntityField(name='closed_at'),
        SchemaEntityField(name='repository_id'),
        SchemaEntityField(name='user_id'),
        SchemaEntityField(name='milestone_id'),
        SchemaEntityField(name='pull_request'),
        SchemaEntityField(name='_fivetran_synced')
    ])

issue_comment_entity = SchemaEntity(name="issue_comment", description="", fields=[
        SchemaEntityField(name='id'),
        SchemaEntityField(name='issue_id'),
        SchemaEntityField(name='body'),
        SchemaEntityField(name='created_at'),
        SchemaEntityField(name='updated_at'),
        SchemaEntityField(name='user_id'),
        SchemaEntityField(name='_fivetran_synced')
    ])

user_entity = SchemaEntity(name="user", description="", fields=[
        SchemaEntityField(name='id', description=''),
        SchemaEntityField(name='login', description=''),
        SchemaEntityField(name='type', description=''),
        SchemaEntityField(name='site_admin', description=''),
        SchemaEntityField(name='name', description=''),
        SchemaEntityField(name='company', description=''),
        SchemaEntityField(name='blog', description=''),
        SchemaEntityField(name='location', description=''),
        SchemaEntityField(name='hireable', description=''),
        SchemaEntityField(name='bio', description=''),
        SchemaEntityField(name='created_at', description=''),
        SchemaEntityField(name='updated_at', description=''),
        SchemaEntityField(name='_fivetran_synced', description='')
    ])
entities = [issue_entity, issue_comment_entity, user_entity]

has_relation = SchemaRelation(name='has', description='')
created_relation = SchemaRelation(name='created', description='')
relations = [has_relation, created_relation]

patterns = [
    SchemaTriplePattern(head=issue_entity, relation=has_relation, tail=issue_comment_entity, description=''),
    SchemaTriplePattern(head=user_entity, relation=created_relation, tail=issue_entity, description=''),
    SchemaTriplePattern(head=user_entity, relation=created_relation, tail=issue_comment_entity, description=''),
]


schema = client.schemas.create(
    workspace_id=workspace.workspace_id,
    name="github",
    entities=entities,
    relations=relations,
    patterns=patterns,
)

print(schema)

