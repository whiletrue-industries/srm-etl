import hashlib
from pathlib import Path

from openlocationcode import openlocationcode as olc
from pyproj import Transformer

import dataflows as DF
from dataflows_airtable import load_from_airtable, dump_to_airtable

from srm_tools.processors import update_mapper
from srm_tools.logger import logger
from srm_tools.update_table import airtable_updater
from srm_tools.unwind import unwind
from srm_tools.hash import hasher

from conf import settings

FILENAME = Path(__file__).resolve().with_name('data.csv')
transformer = Transformer.from_crs('EPSG:2039', 'EPSG:4326', always_xy=True)


def alternate_address(row):
    assert '999' not in row['address'], str(row)
    bad_address = any(not row[f] for f in ['Street', 'House_Num', 'City'])
    has_coord = all(row[f] for f in ['GisX', 'GisY'])
    if bad_address and has_coord:
        x = int(row['GisX'])
        y = int(row['GisY'])
        lon, lat = transformer.transform(x, y)
        pc = olc.encode(lon, lat, 11)
        return pc
    return row['address']


def good_company(r):
    return r['organization_id'] is not None and len(r['organization_id']) == 9


def operator(*_):
    logger.info('Starting Meser Data Flow')

    tags = DF.Flow(
        load_from_airtable(settings.AIRTABLE_ENTITIES_IMPORT_BASE, 'meser-tagging', settings.AIRTABLE_VIEW, settings.AIRTABLE_API_KEY),
        DF.filter_rows(lambda r: r['tag'] != 'dummy'),
        DF.select_fields(['tag', 'response_ids', 'situation_ids']),
    ).results()[0][0]
    tags = {r.pop('tag'): r for r in tags}

    DF.Flow(
        # Loading data
        DF.load(str(FILENAME), infer_strategy=DF.load.INFER_STRINGS),
        DF.update_resource(-1, name='meser'),
        DF.select_fields(['NAME',
                          'Misgeret_Id', 'Type', 'Target_Population', 'Second_Classific', 'ORGANIZATIONS_BUSINES_NUM', 'Registered_Business_Id',
                          'Gender_Descr', 'City', 'Street', 'House_Num', 'Telephone', 'GisX', 'GisY']),
        # Cleanup
        DF.update_schema(-1, missingValues=['NULL', '-1', 'לא ידוע', 'לא משויך', 'רב תכליתי', '0', '999', '9999']),
        DF.validate(),
        DF.set_type('House_Num', type='integer', constraints=dict(maximum=998), on_error=DF.schema_validator.clear),
        DF.set_type('House_Num', type='string', transform=lambda v: str(v) if v else None),
        DF.set_type('Street', type='string', transform=lambda v, row: v if v != row['City'] else None),

        # Adding fields
        DF.add_field('service_name', 'string', lambda r: r['NAME'].strip()),
        DF.add_field('branch_name', 'string', lambda r: r['Type'].strip()),
        DF.add_field('service_description', 'string', lambda r: r['Type'].strip() + (' עבור ' + r['Target_Population'].strip()) if r['Target_Population'] else ''),
        DF.add_field('organization_id', 'string', lambda r: r['ORGANIZATIONS_BUSINES_NUM'] or r['Registered_Business_Id'] or '53a2e790-87b3-44a2-a5f2-5b826f714775'),
        DF.add_field('address', 'string', lambda r: ' '.join(filter(None, [r['Street'], r['House_Num'], r['City']])).replace(' - ', '-')),
        DF.add_field('branch_id', 'string', lambda r: 'meser-' + hasher(r['address'], r['organization_id'])),
        DF.add_field('location', 'string', alternate_address),
        DF.add_field('tagging', 'array', lambda r: list(filter(None, [r['Type'], r['Target_Population'], r['Second_Classific'], r['Gender_Descr']]))),
        DF.add_field('phone_numbers', 'string', lambda r: '0' + r['Telephone'] if r['Telephone'] and r['Telephone'][0] != '0' else r['Telephone'] or None),

        # Combining same services
        DF.add_field('service_id', 'string', lambda r: 'meser-' + hasher(r['service_name'], r['phone_numbers'], r['address'], r['organization_id'], r['branch_id'])),
        DF.join_with_self('meser', ['service_id'], fields=dict(
            service_id=None,
            service_name=None,
            service_description=None,
            branch_id=None,
            branch_name=None,
            organization_id=None,
            address=None,
            location=None,
            tagging=dict(aggregate='array'),
            phone_numbers=None,
        )),
        DF.set_type('tagging', type='array', transform=lambda v: list(set(vvv for vv in v for vvv in vv))),

        # Adding tags
        DF.add_field('responses', 'array', lambda r: list(set(
            r for t in r['tagging'] for r in (tags.get(t, {}).get('response_ids') or [])
        ))),
        DF.add_field('situations', 'array', lambda r: list(set(
            r for t in r['tagging'] for r in (tags.get(t, {}).get('situation_ids') or [])
        ))),

        DF.filter_rows(good_company),

        DF.dump_to_path('temp/meser/denormalized'),
    ).process()

    airtable_updater(
        settings.AIRTABLE_ORGANIZATION_TABLE,
        'entities', ['id'],
        DF.Flow(
            DF.load('temp/meser/denormalized/datapackage.json'),
            DF.join_with_self('meser', ['organization_id'], fields=dict(organization_id=None)),
            DF.rename_fields({'organization_id': 'id'}, resources='meser'),
            DF.add_field('data', 'object', lambda r: dict(id=r['id'])),
            DF.printer()
        ),
        update_mapper(),
        manage_status=False,
        airtable_base=settings.AIRTABLE_ENTITIES_IMPORT_BASE
    )

    airtable_updater(
        settings.AIRTABLE_BRANCH_TABLE,
        'meser', ['id', 'name', 'organization', 'location', 'address', 'phone_numbers'],
        DF.Flow(
            DF.load('temp/meser/denormalized/datapackage.json'),
            DF.join_with_self('meser', ['branch_id'], fields=dict(
                branch_id=None, branch_name=None, organization_id=None, address=None, location=None, phone_numbers=None)
            ),
            DF.rename_fields({
                'branch_id': 'id',
                'branch_name': 'name',
            }, resources='meser'),
            DF.add_field('organization', 'array', lambda r: [r['organization_id']], resources='meser'),
            DF.add_field('data', 'object', lambda r: dict((k,v) for k,v in r.items() if k!='id'), resources='meser'),
            DF.printer()
        ),
        update_mapper(),
        airtable_base=settings.AIRTABLE_ENTITIES_IMPORT_BASE
    )


    DF.Flow(
            DF.load('temp/meser/denormalized/datapackage.json'),
            DF.rename_fields({
                'service_id': 'id',
                'service_name': 'name',
                'service_description': 'description',
            }, resources='meser'),
            DF.add_field('data_sources', 'string', 'מידע על מסגרות רווחה התקבל ממשרד הרווחה והשירותים החברתיים', resources='meser'),
            DF.add_field('branches', 'array', lambda r: [r['branch_id']], resources='meser'),
            DF.select_fields(['id', 'name', 'description', 'data_sources', 'situations', 'responses', 'branches'], resources='meser'),
            DF.add_field('data', 'object', lambda r: dict((k,v) for k,v in r.items() if k!='id'), resources='meser'),
            DF.printer()
    ).process()

    airtable_updater(
        settings.AIRTABLE_SERVICE_TABLE,
        'meser', ['id', 'name', 'description', 'data_sources', 'situations', 'responses', 'branches'],
        DF.Flow(
            DF.load('temp/meser/denormalized/datapackage.json'),
            DF.rename_fields({
                'service_id': 'id',
                'service_name': 'name',
                'service_description': 'description',
            }, resources='meser'),
            DF.add_field('data_sources', 'string', 'מידע על מסגרות רווחה התקבל ממשרד הרווחה והשירותים החברתיים', resources='meser'),
            DF.add_field('branches', 'array', lambda r: [r['branch_id']], resources='meser'),
            DF.select_fields(['id', 'name', 'description', 'data_sources', 'situations', 'responses', 'branches'], resources='meser'),
            DF.add_field('data', 'object', lambda r: dict((k,v) for k,v in r.items() if k!='id'), resources='meser'),
            DF.printer()
        ),
        update_mapper(),
        airtable_base=settings.AIRTABLE_ENTITIES_IMPORT_BASE
    )

    DF.Flow(
        DF.load('temp/meser/denormalized/datapackage.json'),
        DF.update_resource(-1, name='tagging'),
        DF.select_fields(['tagging']),
        unwind('tagging', 'tag'),
        DF.join_with_self('tagging', ['tag'], fields=dict(tag=None)),
        DF.filter_rows(lambda r: r['tag'] not in tags),
        DF.filter_rows(lambda r: bool(r['tag'])),
        dump_to_airtable({
            (settings.AIRTABLE_ENTITIES_IMPORT_BASE, 'meser-tagging'): {
                'resource-name': 'tagging',
            }
        }, settings.AIRTABLE_API_KEY)
    ).process()

    logger.info('Finished Meser Data Flow')


if __name__ == '__main__':
    operator(None, None, None)