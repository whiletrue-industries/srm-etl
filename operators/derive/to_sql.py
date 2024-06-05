import dataflows as DF

from conf import settings
from srm_tools.logger import logger

# TODO: if/when we need a relational Data API
# need foreign key support, just for testing purposes now.
# def relational_sql_flow():
#     return DF.Flow(
#         DF.load(f'{settings.DATA_DUMP_DIR}/srm_data/datapackage.json'),
#         DF.dump_to_sql(
#             dict(
#                 srm_branches={'resource-name': 'branches'},
#                 srm_locations={'resource-name': 'locations'},
#                 srm_organizations={'resource-name': 'organizations'},
#                 srm_responses={'resource-name': 'responses'},
#                 srm_services={'resource-name': 'services'},
#                 srm_situations={'resource-name': 'situations'},
#             )
#         ),
#     )


def dump_to_sql_flow():
    return DF.Flow(
        DF.load(f'{settings.DATA_DUMP_DIR}/cards/datapackage.json'),
        DF.dump_to_sql(
            dict(
                cards={
                    'resource-name': 'cards',
                }
            ), engine='env://DATASETS_DATABASE_URL'
        ),
    )


def operator(*_):
    logger.info('Starting SQL Flow')

    # relational_sql_flow().process()
    dump_to_sql_flow().process()

    logger.info('Finished SQL Flow')


if __name__ == '__main__':
    operator(None, None, None)
