import dataflows as DF

from dataflows_airtable import load_from_airtable, AIRTABLE_ID_FIELD

from conf import settings
from srm_tools.logger import logger
from srm_tools.stats import Report, Stats
from srm_tools.error_notifier import invoke_on
from mde_utils import load_manual_data
from external import main as load_external_data

stats = Stats()

def mde_prepare():

    unknown_entity_ids = Report(
        'Manual Data Entry: No Org ID or Org Name Report',
        'manual-data-entry-no-org-id-or-org-name',
        ['Org Id', 'Org Name', 'Service Name'],
        [AIRTABLE_ID_FIELD]
    )

    source_flow = DF.Flow(
        load_from_airtable(settings.AIRTABLE_DATAENTRY_BASE, settings.AIRTABLE_SERVICE_TABLE, settings.AIRTABLE_VIEW, settings.AIRTABLE_API_KEY),
        DF.filter_rows(lambda r: r.get('Org Id') != 'dummy'),
        stats.filter_with_stat('Manual Data Entry: Entry not ready to publish', lambda r: r.get('Status') == 'בייצור'),
        stats.filter_with_stat('Manual Data Entry: No Org ID or Org Name', lambda r: (r.get('Org Id') or r.get('Org Name')), report=unknown_entity_ids),
        DF.add_field('_row_id', 'string', lambda r: r[AIRTABLE_ID_FIELD]),
    )

    data_sources = DF.Flow(
        load_from_airtable(settings.AIRTABLE_DATAENTRY_BASE, 'DataReferences', settings.AIRTABLE_VIEW, settings.AIRTABLE_API_KEY),
    ).results()[0][0]
    data_sources = dict((r['name'], r['reference']) for r in data_sources)
    return source_flow, data_sources


def run(*_):
    logger.info('Starting Manual Data Entry Flow')
        
    source_flow, data_sources = mde_prepare()
    load_manual_data(source_flow, data_sources)

    logger.info('Finished Manual Data Entry Flow')

    logger.info('Starting External Manual Data Entry Flow')

    load_external_data()

    logger.info('Finished External Manual Data Entry Flow')


def operator(*_):
    invoke_on(run, 'Manual Data Entry')

if __name__ == '__main__':
    operator(None, None, None)
