import shutil

import datetime

import dataflows as DF
from slugify import slugify

from conf import settings

from operators.derive.autocomplete import VERIFY_ORG_ID

from srm_tools.hash import hasher
from srm_tools.update_table import airtable_updater
from srm_tools.url_utils import fix_url

CHECKPOINT = 'mde'

# PREPARE
def slugify_org_id():
    def func(row):
        id = row.get('Org Id') or row.get('Org Name')
        row['Org Id'] = slugify(id, separator='-', lowercase=True)
        if VERIFY_ORG_ID.match(row['Org Id']) is None:
            h = hasher(row['Org Id'])
            i = int(h, 16)
            row['Org Id'] = f'srm9{i}'
    return func


def handle_national_services():
    def func(row):
        if row.get('National Service?'):
            row['Branch Address'] = 'שירות ארצי'
    return func


def handle_no_taxonomies():
    def func(row):
        data = row['data']
        if not data.get('responses'):
            data.pop('responses', None)
        if not data.get('situations'):
            data.pop('situations', None)
    return func

# ORGS
def org_updater():
    def func(row):
        if not row.get('data'):
            return
        data = row['data']
        if row.get('urls'):
            urls = row['urls'].split('\n')
        else:
            urls = []
        if data['urls']:
            new_urls = data['urls'].split('\n')
            for new_url in new_urls:
                new_url = fix_url(new_url)
                if new_url:
                    new_url = new_url + '#אתר הבית'
                    if new_url not in urls:
                        urls.append(new_url)
        row['urls'] = '\n'.join(urls)
        row['phone_numbers'] = data['phone_numbers']
        row['email_address'] = data['email_address']
        row['last_tag_date'] = data['last_tag_date']
    return func


def mde_organization_flow():
    today = datetime.date.today().isoformat()
    orgs = DF.Flow(
        DF.checkpoint(CHECKPOINT),
        DF.update_resource(-1, name='orgs'),
        DF.select_fields(['Org Id', 'Org Phone Number', 'Org Website', 'Org Email']),
        DF.rename_fields({
            'Org Id': 'id',
            'Org Phone Number': 'phone_numbers',
            'Org Website': 'urls',
            'Org Email': 'email_address',
        }),
        DF.join_with_self('orgs', ['id'], dict(
            id=None, urls=None, phone_numbers=None, email_address=None
        )),
        DF.add_field('data', 'object', lambda r: dict(
            urls=r['urls'],
            phone_numbers=r['phone_numbers'],
            email_address=r['email_address'],
            last_tag_date=today,
        )),
        DF.select_fields(['id', 'data']),
        DF.printer()
    ).results()[0][0]

    print('COLLECTED {} relevant organizations'.format(len(orgs)))

    airtable_updater(settings.AIRTABLE_ORGANIZATION_TABLE, 'entities',
        ['urls', 'phone_numbers', 'last_tag_date'],
        orgs,
        org_updater(), 
        manage_status=False,
        airtable_base=settings.AIRTABLE_DATA_IMPORT_BASE
    )

# BRANCHES
def mde_id(*args):
    return 'mde:' + hasher(*map(str, args))

def mde_branch_id(row, prefix=''):
    return mde_id(row['organization'], row['operating_unit'], row.get(prefix + 'address'), row.get(prefix + 'geocode'))

def branch_updater():
    def func(row):
        if not row.get('data'):
            return
        data = row['data']
        data['location'] = data.get('geocode') or data.get('address')
        if data.get('location'):
            data['location'] = data['location'].strip()
        urls = []
        combined = []
        if data.get('urls'):
            urls = data['urls'].split('\n')
        for url in urls:
            url = fix_url(url)
            if url:
                combined.append(url + '#אתר הסניף')
        data['urls'] = '\n'.join(combined)
        row.update(data)

    return func


def mde_branch_flow(source_id, branch_ids):

    def update_branch_ids(row):
        branch_ids[row['_id']] = row['id']

    branches = DF.Flow(
        DF.checkpoint(CHECKPOINT),
        DF.update_resource(-1, name='branches'),
        DF.select_fields(['Org Id', 'Org Name', 'Org Short Name', 'Branch Details', 'Branch Address', 'Branch Geocode', 
                          'Branch Phone Number', 'Branch Email', 'Branch Website', '_row_id']),
        DF.rename_fields({
            'Org Id': 'organization',
            'Branch Details': 'name',
            'Branch Address': 'address',
            'Branch Geocode': 'geocode',
            'Branch Phone Number': 'phone_numbers',
            'Branch Email': 'email_address',
            'Branch Website': 'urls',
        }),
        DF.add_field('operating_unit', 'string', lambda r: r.get('Org Short Name') or r.get('Org Name')),
        DF.delete_fields(['Org Name', 'Org Short Name']),
        DF.add_field('_id', 'string', lambda r: mde_branch_id(r)),
        DF.set_type('_row_id', type='string', transform=lambda v: mde_id(v)),
        DF.join_with_self('branches', ['_id'], dict(
            _id=None,
            id=dict(name='_row_id', aggregate='min'),
            name=None, 
            operating_unit=None,
            address=None,
            geocode=None, 
            phone_numbers=None,
            email_address=None,
            urls=None,
            organization=None,
        )),
        update_branch_ids,
        DF.add_field('data', 'object', lambda r: dict(
            name=r['name'],
            operating_unit=r['operating_unit'],
            address=r['address'],
            geocode=r['geocode'],
            phone_numbers=r['phone_numbers'],
            email_address=r['email_address'],
            urls=r['urls'],
            organization=[r['organization']],
        )),
        DF.select_fields(['id', 'data']),
    ).results()[0][0]

    print('COLLECTED {} relevant branches'.format(len(branches)))
    return airtable_updater(settings.AIRTABLE_BRANCH_TABLE, source_id,
        ['id', 'name', 'organization', 'operating_unit', 'location', 'address', 'phone_numbers', 'email_address', 'urls'],
        branches,
        branch_updater(),
        airtable_base=settings.AIRTABLE_DATA_IMPORT_BASE
    )


# SERVICES
def service_updater(data_sources):

    def func(row):
        if not row.get('data'):
            return
        data = row['data']
        urls = []
        combined = []
        if data.get('urls'):
            urls = data['urls'].split('\n')
        for url in urls:
            url = fix_url(url)
            if url:
                combined.append(url + '#אתר השירות')
        data['urls'] = '\n'.join(combined)
        data['data_sources'] = data_sources.get(data['data_source'], '')
        row.update(data)

    return func

def mde_service_flow(data_sources, source_id, branch_ids):

    # data_sources = DF.Flow(
    #     load_from_airtable(settings.AIRTABLE_DATAENTRY_BASE, 'DataReferences', settings.AIRTABLE_VIEW, settings.AIRTABLE_API_KEY),
    # ).results()[0][0]
    # data_sources = dict((r['name'], r['reference']) for r in data_sources)

    services = DF.Flow(
        DF.checkpoint(CHECKPOINT),
        DF.update_resource(-1, name='services'),
        DF.select_fields(['Org Id', 'Org Name', 'Org Short Name', 'Branch Address', 'Branch Geocode', 'Data Source',
                          'Service Name', 'Service Description', 'Service Conditions', 'Service Phone Number', 'Service Email', 'Service Website',
                          'responses_ids', 'situations_ids', 'target_audiences', 'notes', '_row_id']),
        DF.rename_fields({
            'Org Id': 'organization',
            'Data Source': 'data_source',
            'Branch Address': 'branch_address',
            'Branch Geocode': 'branch_geocode',
            'Service Name': 'name',
            'Service Description': 'description',
            'Service Conditions': 'payment_details',
            'Service Phone Number': 'phone_numbers',
            'Service Email': 'email_address',
            'Service Website': 'urls',
        }),
        DF.add_field('operating_unit', 'string', lambda r: r.get('Org Short Name') or r.get('Org Name')),
        DF.add_field('branch_id', 'string', lambda r: branch_ids[mde_branch_id(r, prefix='branch_')]),
        DF.add_field('data', 'object', lambda r: dict(
            name=r['name'],
            description=r.get('description'),
            payment_details=r.get('payment_details'),
            urls=r.get('urls'),
            branches=[r['branch_id']],
            responses=r.get('responses_ids'),
            situations=r.get('situations_ids'),
            phone_numbers=r.get('phone_numbers'),
            data_source=r['data_source'],
            email_address=r.get('email_address'),
            target_audiences=r.get('target_audiences'),
            notes=r.get('notes'),
        )),
        handle_no_taxonomies(),
        DF.add_field('id', 'string', lambda r: mde_id(r['branch_id'], r['_row_id'])),
        DF.select_fields(['id', 'data']),
    ).results()[0][0]

    print('COLLECTED {} relevant services'.format(len(services)))
    airtable_updater(settings.AIRTABLE_SERVICE_TABLE, source_id,
        ['id', 'name', 'description', 'payment_details', 'phone_numbers', 'email_address', 'urls', 'situations', 'responses', 'branches', 'data_sources', 'target_audiences', 'notes'],
        services,
        service_updater(data_sources),
        airtable_base=settings.AIRTABLE_DATA_IMPORT_BASE
    )


def load_manual_data(source_flow, data_sources, source_id='manual-data-entry'):
    shutil.rmtree(f'.checkpoints/{CHECKPOINT}', ignore_errors=True, onerror=None)

    DF.Flow(
        source_flow,
        slugify_org_id(),
        handle_national_services(),
        DF.checkpoint(CHECKPOINT)
    ).process()

    mde_organization_flow()
    branch_ids = {}
    mde_branch_flow(source_id, branch_ids)
    mde_service_flow(data_sources, source_id, branch_ids)
