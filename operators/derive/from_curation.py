import dataflows as DF

from srm_tools.logger import logger
from srm_tools.processors import fetch_mapper, update_mapper
from srm_tools.update_table import airtable_updater
from dataflows_airtable import load_from_airtable, AIRTABLE_ID_FIELD, dump_to_airtable

from conf import settings


def filter_by_items(mapping, fields):
    def func(rows):
        if rows.res.name == 'current':
            yield from rows
        else:
            for row in rows:
                items = None
                for f in fields:
                    items = items or row.get(f)
                if items:
                    for i in range(len(items)):
                        item = items.pop(0)
                        if item in mapping:
                            items.append(mapping[item])
                yield row
    return func


def collect_ids(mapping, ignore_ids=set()):
    def func(rows):
        if rows.res.name == 'current':
            yield from rows
        else:
            for row in rows:
                if row['id'] not in ignore_ids:
                    mapping[row.get(AIRTABLE_ID_FIELD)] = row['id']
                    yield row
    return func


def copy_from_curation_base(curation_base, source_id, ignore_orgs=set()):
    logger.info(f'COPYING Data from {curation_base}')
    updated_orgs = dict()
    updated_branches = dict()

    for table in (settings.AIRTABLE_ORGANIZATION_TABLE, settings.AIRTABLE_BRANCH_TABLE, settings.AIRTABLE_SERVICE_TABLE):
        print('FIXING NEWS', curation_base, table, source_id)
        DF.Flow(
            load_from_airtable(curation_base, table, settings.AIRTABLE_VIEW, settings.AIRTABLE_API_KEY),
            DF.filter_rows(lambda r: not r.get('decision')),
            DF.set_type('decision', transform=lambda v: v or 'New'),
            DF.select_fields(['id', 'decision', AIRTABLE_ID_FIELD],),
            DF.update_resource(-1, name='current'),
            dump_to_airtable({
                (curation_base, table): {
                    'resource-name': 'current',
                }
            }, settings.AIRTABLE_API_KEY),
        ).process()

    airtable_updater(settings.AIRTABLE_ORGANIZATION_TABLE, source_id,
        ['name', 'short_name', 'kind', 'urls', 'phone_numbers', 'description', 'purpose'],
        DF.Flow(
            load_from_airtable(curation_base, settings.AIRTABLE_ORGANIZATION_TABLE, settings.AIRTABLE_VIEW, settings.AIRTABLE_API_KEY),
            DF.update_resource(-1, name='orgs'),
            DF.filter_rows(lambda r: r['status'] == 'ACTIVE', resources='orgs'),
            DF.filter_rows(lambda r: r['decision'] not in ('Rejected', 'Suspended'), resources='orgs'),
            collect_ids(updated_orgs, ignore_orgs),
            DF.delete_fields(['source', 'status'], resources=-1),
            fetch_mapper(),
        ),
        update_mapper()
    )
    print('UPDATED ORGS', list(updated_orgs.values())[:10])
    conversion = dict()
    DF.Flow(
        load_from_airtable(settings.AIRTABLE_BASE, settings.AIRTABLE_ORGANIZATION_TABLE, settings.AIRTABLE_VIEW, settings.AIRTABLE_API_KEY),
        lambda row: conversion.setdefault(row['id'], row.get(AIRTABLE_ID_FIELD)),
    ).process()
    updated_orgs = {k: conversion.get(v) for k, v in updated_orgs.items()}

    updated_locations = dict()
    DF.Flow(
        load_from_airtable(settings.AIRTABLE_BASE, settings.AIRTABLE_LOCATION_TABLE, settings.AIRTABLE_VIEW, settings.AIRTABLE_API_KEY),
        lambda row: updated_locations.setdefault(row['id'], row.get(AIRTABLE_ID_FIELD)),
    ).process()

    airtable_updater(settings.AIRTABLE_BRANCH_TABLE, source_id,
        ['name', 'organization', 'address', 'address_details', 'location', 'description', 'phone_numbers', 'urls', 'situations'],
        DF.Flow(
            load_from_airtable(curation_base, settings.AIRTABLE_BRANCH_TABLE, settings.AIRTABLE_VIEW, settings.AIRTABLE_API_KEY),
            DF.update_resource(-1, name='branches'),
            DF.filter_rows(lambda r: r['status'] == 'ACTIVE', resources='branches'),
            DF.filter_rows(lambda r: r['decision'] not in ('Rejected', 'Suspended'), resources='branches'),
            DF.set_type('location', type='array', transform=lambda v: [updated_locations.get(v, v)]),
            filter_by_items(updated_orgs, ['organization']),
            DF.filter_rows(lambda r: len(r['organization'] or []) > 0),
            collect_ids(updated_branches),
            DF.delete_fields(['source', 'status'], resources=-1),
            fetch_mapper(),
        ),
        update_mapper()
    )
    print('UPDATED BRANCHES', list(updated_branches.values())[:10])
    conversion = dict()
    DF.Flow(
        load_from_airtable(settings.AIRTABLE_BASE, settings.AIRTABLE_BRANCH_TABLE, settings.AIRTABLE_VIEW, settings.AIRTABLE_API_KEY),
        lambda row: conversion.setdefault(row['id'], row.get(AIRTABLE_ID_FIELD)),
    ).process()
    updated_branches = {k: conversion.get(v) for k, v in updated_branches.items()}

    airtable_updater(settings.AIRTABLE_SERVICE_TABLE, source_id,
        ['name', 'description', 'details', 'payment_required', 'payment_details', 'urls', 'phone_numbers',
         'situations', 'responses', 'organizations', 'branches', 'responses_manual', 'situations_manual', 'data_sources'],
        DF.Flow(
            load_from_airtable(curation_base, settings.AIRTABLE_SERVICE_TABLE, settings.AIRTABLE_VIEW, settings.AIRTABLE_API_KEY),
            DF.update_resource(-1, name='services'),
            DF.filter_rows(lambda r: r['status'] == 'ACTIVE', resources='services'),
            DF.filter_rows(lambda r: r['decision'] not in ('Rejected', 'Suspended'), resources='services'),
            filter_by_items(updated_orgs, ['organizations']),
            filter_by_items(updated_branches, ['branches']),
            DF.filter_rows(lambda r: len(r['organizations'] or []) > 0 or len(r['branches'] or []) > 0),
            DF.delete_fields(['source', 'status'], resources=-1),
            fetch_mapper(),
        ),
        update_mapper()
    )
    return set(updated_orgs.values())
    

def operator(*_):
    logger.info('Copying data from curation tables')
    entity_ids = copy_from_curation_base(settings.AIRTABLE_ENTITIES_IMPORT_BASE, 'entities')
    copy_from_curation_base(settings.AIRTABLE_GUIDESTAR_IMPORT_BASE, 'guidestar', entity_ids)
    logger.info('Finished Copying data from curation tables')


if __name__ == '__main__':
    operator(None, None, None)
