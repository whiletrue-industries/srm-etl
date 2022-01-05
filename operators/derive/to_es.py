import tempfile
import shutil
from dataflows_airtable.load_from_airtable import load_from_airtable
import requests

import dataflows as DF
from dataflows_ckan import dump_to_ckan
import yaml

from conf import settings

from . import helpers
from .es_utils import dump_to_es_and_delete

from srm_tools.logger import logger



def data_api_es_flow():
    return DF.Flow(
        DF.load(f'{settings.DATA_DUMP_DIR}/card_data/datapackage.json'),
        DF.update_package(title='Card Data', name='srm_card_data'),
        DF.update_resource('card_data', name='cards'),
        DF.add_field('score', 'number', 1),
        DF.set_type('card_id', **{'es:keyword': True}),
        DF.set_type('branch_id', **{'es:keyword': True}),
        DF.set_type('service_id', **{'es:keyword': True}),
        DF.set_type('organization_id', **{'es:keyword': True}),
        DF.set_type('response_categories', **{'es:itemType': 'string', 'es:keyword': True}),
        DF.set_type(
            'situations',
            **{
                'es:itemType': 'object',
                'es:schema': {
                    'fields': [
                        {'type': 'string', 'name': 'id', 'es:keyword': True},
                        {'type': 'string', 'name': 'name'},
                    ]
                },
            },
        ),
        DF.set_type(
            'responses',
            **{
                'es:itemType': 'object',
                'es:schema': {
                    'fields': [
                        {'type': 'string', 'name': 'id', 'es:keyword': True},
                        {'type': 'string', 'name': 'name'},
                    ]
                },
            },
        ),
        DF.set_type(
            'service_urls',
            **{
                'es:itemType': 'object',
                'es:index': False,
                'es:schema': {
                    'fields': [
                        {'type': 'string', 'name': 'href'},
                        {'type': 'string', 'name': 'text'},
                    ]
                },
            },
        ),
        DF.set_type(
            'branch_urls',
            **{
                'es:itemType': 'object',
                'es:index': False,
                'es:schema': {
                    'fields': [
                        {'type': 'string', 'name': 'href'},
                        {'type': 'string', 'name': 'text'},
                    ]
                },
            },
        ),
        DF.set_type(
            'organization_urls',
            **{
                'es:itemType': 'object',
                'es:index': False,
                'es:schema': {
                    'fields': [
                        {'type': 'string', 'name': 'href'},
                        {'type': 'string', 'name': 'text'},
                    ]
                },
            },
        ),
        DF.set_type(
            'branch_email_addresses',
            **{
                'es:itemType': 'string',
                'es:index': False,
            },
        ),
        DF.set_type(
            'branch_phone_numbers',
            **{
                'es:itemType': 'string',
                'es:index': False,
            },
        ),
        DF.set_type(
            'response_ids',
            **{
                'es:itemType': 'string',
                'es:keyword': True,
            },
        ),
        DF.set_type(
            'situation_ids',
            **{
                'es:itemType': 'string',
                'es:keyword': True,
            },
        ),
        dump_to_es_and_delete(
            indexes=dict(srm__cards=[dict(resource_name='cards')]),
        ),
        dump_to_ckan(
            settings.CKAN_HOST,
            settings.CKAN_API_KEY,
            settings.CKAN_OWNER_ORG,
        ),
    )

def load_locations_to_es_flow():
    url = settings.LOCATION_BOUNDS_SOURCE_URL
    scores = dict(
        city=100, town=50, village=10, hamlet=5,
    )
    def calc_score(r):
        b = r['bounds']
        size = (b[2] - b[0]) * (b[3] - b[1]) * 100000
        return size * scores.get(r['place'], 1)

    with tempfile.NamedTemporaryFile(suffix='.zip', delete=False) as tmpfile:
        src = requests.get(url, stream=True).raw
        shutil.copyfileobj(src, tmpfile)
        tmpfile.close()
        return DF.Flow(
            DF.load(tmpfile.name, format='datapackage'),
            DF.update_package(title='Bounds for Locations in Israel', name='bounds-for-locations'),
            DF.update_resource(-1, name='places'),
            DF.set_type('name', **{'es:autocomplete': True}),
            DF.add_field('score', 'number', calc_score),
            DF.dump_to_path(f'{settings.DATA_DUMP_DIR}/place_data'),
            dump_to_es_and_delete(
                indexes=dict(srm__places=[dict(resource_name='places')]),
            ),
            dump_to_ckan(
                settings.CKAN_HOST,
                settings.CKAN_API_KEY,
                settings.CKAN_OWNER_ORG,
            ),
        )

def load_responses_to_es_flow():
    
    def print_top(row):
        parts = row['id'].split(':')
        if len(parts) == 2:
            print('STATS', parts[1], row['count'])

    return DF.Flow(
        DF.load(f'{settings.DATA_DUMP_DIR}/card_data/datapackage.json'),
        DF.add_field('response_ids', 'array', lambda r: [r['id'] for r in r['responses']]),
        DF.set_type('response_ids', transform=lambda v: helpers.update_taxonomy_with_parents(v)),
        DF.select_fields(['response_ids']),
        helpers.unwind('response_ids', 'id', 'object'),
        DF.join_with_self('card_data', ['id'], dict(
            id=None,
            count=dict(aggregate='count')
        )),
        load_from_airtable(settings.AIRTABLE_BASE, settings.AIRTABLE_RESPONSE_TABLE, settings.AIRTABLE_VIEW, settings.AIRTABLE_API_KEY),
        DF.update_package(title='Taxonomy Responses', name='responses'),
        DF.update_resource(-1, name='responses'),
        DF.join('card_data', ['id'], 'responses', ['id'], dict(
            count=None
        )),
        DF.filter_rows(lambda r: r['status'] == 'ACTIVE'),
        DF.filter_rows(lambda r: r['count'] is not None),
        DF.select_fields(['id', 'name', 'breadcrumbs', 'count']),
        DF.set_type('id', **{'es:keyword': True}),
        DF.set_type('name', **{'es:autocomplete': True}),
        DF.add_field('score', 'number', lambda r: r['count']),
        DF.set_primary_key(['id']),
        print_top,
        DF.dump_to_path(f'{settings.DATA_DUMP_DIR}/response_data'),
        dump_to_es_and_delete(
            indexes=dict(srm__responses=[dict(resource_name='responses')]),
        ),
        DF.update_resource(-1, name='responses', path='responses.json'),
        dump_to_ckan(
            settings.CKAN_HOST,
            settings.CKAN_API_KEY,
            settings.CKAN_OWNER_ORG,
            format='json'
        ),
        # DF.printer()
    )


def load_situations_flow():

    OPENELIGIBILITY_YAML_URL = 'https://raw.githubusercontent.com/hasadna/openeligibility/main/taxonomy.tx.yaml'
    taxonomy = requests.get(OPENELIGIBILITY_YAML_URL).text
    taxonomy = yaml.safe_load(taxonomy)
    situations = [t for t in taxonomy if t['slug'] == 'human_situations'][0]['items']

    return DF.Flow(
        situations,
        DF.update_package(title='Taxonomy Situations', name='situations'),
        DF.update_resource(-1, name='situations', path='situations.json'),
        dump_to_ckan(
            settings.CKAN_HOST,
            settings.CKAN_API_KEY,
            settings.CKAN_OWNER_ORG,
            force_format=False
        ),
        DF.printer()
    )


def operator(*_):
    logger.info('Starting ES Flow')
    data_api_es_flow().process()
    load_locations_to_es_flow().process()
    load_responses_to_es_flow().process()
    load_situations_flow().process()
    logger.info('Finished ES Flow')


if __name__ == '__main__':
    operator(None, None, None)
