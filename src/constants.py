SLI_CON_REV_DB = 'sli_consensus_revision'
SLI_REV_DB = 'sli_revision'
SLI_VAACTUALS_DB = 'sli_vaactuals_revision'

SLI_CON_LATEST_DB = 'sli_consensus_latest'
SLI_LATEST_DB = 'sli_latest'
SLI_VAACTUALS_LATEST = 'sli_vaactuals_latest'

SLI_DB = 'sli'

SOURCE_CLIENT = 'mongo_source'
DESTINATION_CLIENT = 'mongo_dest'

MYSQL_DB_URL = "mysql://{0}:{1}@{2}/{3}"
MONGO_DB_URL = "mongodb://{0}:{1}/"

MONGO_DB_REMOTE_URL = "mongodb://{user}:{password}@{host}:{port}/sliconsensusrevision"

SOURCE_HOST = '127.0.0.1'
SOURCE_PORT = '27017'

DESTINATION_HOST = '127.0.0.1'
DESTINATION_PORT = '27020'

DATABASE = 'sliconsensusrevision'

# DB_TO_MIGRATE = {
#     'sliconsensusrevision': "revisiondpid, expression, computeinfojson",
#     'slirevision': "revisiondpid, expression",
#     'slivaactualsrevision': "revisiondpid, expression, computeinfojson",
# }

test_revisiondpids = (9529232226, 8117414530, 8117415374)

