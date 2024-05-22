import datetime
import dateutil.parser

import dataflows as DF
from dataflows.base.resource_wrapper import ResourceWrapper

from dataflows_airtable import dump_to_airtable, load_from_airtable
from dataflows_airtable.consts import AIRTABLE_ID_FIELD

from openlocationcode import openlocationcode as olc

from srm_tools.update_table import airtable_updater
from srm_tools.guidestar_api import GuidestarAPI
from srm_tools.budgetkey import fetch_from_budgetkey
from srm_tools.data_cleaning import clean_org_name
from srm_tools.processors import update_mapper

from conf import settings
from srm_tools.logger import logger
from srm_tools.url_utils import fix_url



## ORGANIZATIONS
def fetchEntityFromBudgetKey(regNum):
    entity = list(fetch_from_budgetkey(f"select * from entities where id='{regNum}'"))
    if len(entity) > 0:
        entity = entity[0]
        name = entity['name']
        rec = dict(
            id=entity['id'],
            data = dict(
                name=name,
                kind=entity['kind_he'],
                purpose=entity['details'].get('goal'),
            )
        )
        return rec


def updateOrgFromSourceData(ga: GuidestarAPI):
    def func(rows):
        for row in rows:
            regNums = [row['id']]
            if row['id'].startswith('srm'):
                yield row
                continue
            # if row['kind'] is not None:
            #     continue
            for data in ga.organizations(regNums=regNums, cacheOnly=True):
                try:
                    data = data['data']
                    row['name'] = data['name'].replace(' (חל"צ)', '').replace(' (ע"ר)', '')
                    row['short_name'] = data.get('abbreviatedOrgName')
                    row['kind'] = data['malkarType']
                    row['description'] = None
                    row['purpose'] = data.get('orgGoal')
                    urls = []
                    if data.get('website'):
                        website = fix_url(data['website'])
                        if website:
                            urls.append(website + '#אתר הבית')
                    row['urls'] = '\n'.join(urls)
                    phone_numbers = []
                    if data.get('tel1'):
                        phone_numbers.append(data['tel1'])
                    if data.get('tel2'):
                        phone_numbers.append(data['tel2'])
                    row['phone_numbers'] = '\n'.join(phone_numbers)
                    if data.get('email'):
                        row['email_address'] = data['email']
                    break
                except Exception as e:
                    print('BAD DATA RECEIVED', str(e), regNums, data)
            else:
                data = fetchEntityFromBudgetKey(row['id'])
                if data is not None:
                    row.update(data['data'])
                # else:
                #     print('NOT FOUND', regNums)
            yield row
    return func

def recent_org(row):
    ltd = row.get('last_tag_date')
    if ltd is not None:
        ltd = dateutil.parser.isoparse(ltd)
        days = (ltd.now() - ltd).days
        if days < 15:
            return True
    return False


def fetchOrgData(ga):
    DF.Flow(
        load_from_airtable(settings.AIRTABLE_DATA_IMPORT_BASE, settings.AIRTABLE_ORGANIZATION_TABLE, settings.AIRTABLE_VIEW, settings.AIRTABLE_API_KEY),
        DF.update_resource(-1, name='orgs'),
        DF.filter_rows(lambda row: row.get('source') == 'entities'),
        DF.select_fields([AIRTABLE_ID_FIELD, 'id', 'kind']),
        updateOrgFromSourceData(ga),
        dump_to_airtable({
            (settings.AIRTABLE_DATA_IMPORT_BASE, settings.AIRTABLE_ORGANIZATION_TABLE): {
                'resource-name': 'orgs',
            }
        }, settings.AIRTABLE_API_KEY)
    ).process()


## BRANCHES
def unwind_branches(ga:GuidestarAPI):
    def func(rows: ResourceWrapper):
        if rows.res.name != 'orgs':
            yield from rows        
        else:
            for _, row in enumerate(rows):
                regNum = row['id']
                branches = ga.branches(regNum)
                for branch in branches:
                    ret = dict()
                    ret.update(row)

                    data = dict()
                    if branch.get('placeNickname'):
                        data['name'] = branch['placeNickname']
                    else:
                        data['name'] = (ret.get('short_name') or ret.get('name')) + ' - ' + branch['cityName']
                    data['address'] = calc_address(branch)
                    data['location'] = calc_location_key(branch, data)
                    data['address_details'] = branch.get('drivingInstructions')
                    data['description'] = None
                    data['urls'] = None
                    # if data.get('branchURL'):
                    #     row['urls'] = data['branchURL'] + '#הסניף בגיידסטאר'
                    data['phone_numbers'] = None
                    if branch.get('phone'):
                        data['phone_numbers'] = branch['phone']
                    data['organization'] = [regNum]
                    if branch.get('language'):
                        data['situations'] = [
                            'human_situations:language:{}_speaking'.format(l.lower().strip()) for l in branch['language'].split(';')
                        ]

                    ret['data'] = data
                    ret['id'] = 'guidestar:' + branch['branchId']
                    yield ret
                if not branches:
                    # print('FETCHING FROM GUIDESTAR', regNum)
                    ret = list(ga.organizations(regNums=[regNum], cacheOnly=True))
                    if len(ret) > 0 and ret[0]['data'].get('fullAddress'):
                        data = ret[0]['data']
                        yield dict(
                            id='guidestar:' + regNum,
                            data=dict(
                                name=row['name'],
                                address=data['fullAddress'],
                                location=data['fullAddress'],
                                organization=[regNum]
                            )
                        )
                    else:
                        if ret:
                            if row['kind'] not in ('עמותה', 'חל"צ', 'הקדש'):
                                print('FETCHING FROM BUDGETKEY', regNum, ret, row)
                                ret = dict()
                                ret.update(row)
                                name = row['name']
                                cleaned_name = clean_org_name(name)
                                ret.update(dict(
                                    id='budgetkey:' + regNum,
                                    data=dict(
                                        name=name,
                                        address=cleaned_name,
                                        location=cleaned_name,
                                        organization=[regNum]
                                    )
                                ))
                                yield ret
                national = {}
                national.update(row)
                national['id'] = 'guidestar:' + regNum + ':national'
                national['data'] = {
                    'branchId': national['id'],
                    'organization': regNum,
                    'name': row['name'],
                    'address': 'שירות ארצי',
                    'location': 'שירות ארצי',
                }
                yield national

    return DF.Flow(
        DF.add_field('data', 'object', resources='orgs'),
        func,
    )

def calc_address(row):
    key = ''
    cityName = row.get('cityName')
    if cityName:
        cityName = cityName.replace(' תאי דואר', '')
        streetName = row.get('streetName')
        if streetName:
            key += f'{streetName} '
            houseNum = row.get('houseNum')
            if houseNum:
                key += f'{houseNum} '
            key += ', '
        key += f'{cityName} '
    
    alternateAddress = row.get('alternateAddress')
    if alternateAddress and alternateAddress != 'ללא כתובת':
        if alternateAddress not in key:
            key += f' - {alternateAddress}'
    key = key.strip()

    return key or None

def calc_location_key(src, dst):
    y, x = src.get('latitude'), src.get('longitude')
    if y and x:
        code = olc.encode(y, x, 11)
    else:
        code = None
    return code or dst['address']

def updateBranchFromSourceData():
    def func(row):
        data = row.get('data')
        if data is None:
            return
        # print('data', data)
        # print('row', row)
        row.update(data)
    return func


def fetchBranchData(ga):
    print('FETCHING ALL ORGANIZATION BRANCHES')

    DF.Flow(
        load_from_airtable(settings.AIRTABLE_DATA_IMPORT_BASE, settings.AIRTABLE_ORGANIZATION_TABLE, settings.AIRTABLE_VIEW, settings.AIRTABLE_API_KEY),
        DF.update_resource(-1, name='orgs'),
        DF.filter_rows(lambda r: r['source'] == 'entities', resources='orgs'),
        DF.filter_rows(lambda r: r['status'] == 'ACTIVE', resources='orgs'),
        # DF.filter_rows(lambda r: len(r.get('branches') or []) == 0, resources='orgs'),
        DF.select_fields(['id', 'name', 'short_name', 'kind'], resources='orgs'),
        DF.dump_to_path('temp/entities-orgs')
    ).process()

    airtable_updater(settings.AIRTABLE_BRANCH_TABLE, 'entities',
        ['name', 'organization', 'address', 'address_details', 'location', 'description', 'phone_numbers', 'urls', 'situations'],
        DF.Flow(
            DF.load('temp/entities-orgs/datapackage.json'),
            unwind_branches(ga),
        ),
        updateBranchFromSourceData(),
        airtable_base=settings.AIRTABLE_DATA_IMPORT_BASE,
        manage_status=False
    )


## SERVICES
def unwind_services(ga: GuidestarAPI):
    def func(rows: ResourceWrapper):
        if rows.res.name != 'orgs':
            yield from rows        
        else:
            count = 0
            for _, row in enumerate(rows):
                regNum = row['id']

                branches = ga.branches(regNum)
                # if len(branches) == 0:
                #     continue
                services = ga.services(regNum)
                govServices = dict(
                    (s['relatedMalkarService'], s) for s in services if s.get('serviceGovName') is not None and s.get('relatedMalkarService') is not None
                )
                for service in services:
                    if service['serviceId'] in govServices:
                        print('GOT RELATED SERVICE', service['serviceId'])
                        service['relatedMalkarService'] = govServices.get(service['serviceId'])
                    if service.get('recordType') != 'GreenInfo':
                        continue
                    if not service.get('serviceName'):
                        continue
                    ret = dict()
                    ret.update(row)
                    ret['data'] = service
                    ret['data']['organization_id'] = regNum
                    ret['data']['actual_branch_ids'] = [b['branchId'] for b in branches]
                    ret['id'] = 'guidestar:' + service['serviceId']
                    count += 1
                    if count % 1000 == 0:
                        print('COLLECTED {} services'.format(count))
                    yield ret
    return DF.Flow(
        DF.add_field('data', 'object', resources='orgs'),
        func,
        DF.delete_fields(['source', 'status']),
    )


def updateServiceFromSourceData(taxonomies):
    def update_from_taxonomy(names, responses, situations):
        for name in names:
            if name:
                try:
                    mapping = taxonomies[name]
                    responses.update(mapping['response_ids'] or [])
                    situations.update(mapping['situation_ids'] or [])
                except KeyError:
                    print('WARNING: no mapping for {}'.format(name))
                    taxonomies[name] = dict(response_ids=[], situation_ids=[])
                    DF.Flow(
                        [dict(name=name)],
                        DF.update_resource(-1, name='taxonomies'),
                        dump_to_airtable({
                            (settings.AIRTABLE_BASE, settings.AIRTABLE_TAXONOMY_MAPPING_GUIDESTAR_TABLE): {
                                'resource-name': 'taxonomies',
                                'typecast': True
                            }
                        }, settings.AIRTABLE_API_KEY),
                    ).process()

    def func(rows):
        count = 0
        full_count = 0
        for row in rows:
            full_count += 1
            if 'data' not in row:
                yield row
                continue

            data = row['data']

            responses = set()
            situations = set()

            row['name'] = data.pop('serviceName')
            row['description'] = data.pop('voluntaryDescription') or data.pop('description')
            data_source_url = f'https://www.guidestar.org.il/organization/{data["organization_id"]}/services'
            row['data_sources'] = f'מידע נוסף אפשר למצוא ב<a target="_blank" href="{data_source_url}">גיידסטאר - אתר העמותות של ישראל</a>'
            orgId = data.pop('organization_id')
            actual_branch_ids = data.pop('actual_branch_ids')
            row['branches'] = ['guidestar:' + b['branchId'] for b in (data.pop('branches') or []) if b['branchId'] in actual_branch_ids]
            row['organizations'] = []

            record_type = data.pop('recordType')
            assert record_type == 'GreenInfo'
            for k in list(data.keys()):
                if k.startswith('youth'):
                    data.pop(k)

            relatedMalkarService = data.pop('relatedMalkarService') or {}

            if 'serviceTypeNum' in data:
                update_from_taxonomy([data.pop('serviceTypeNum')], responses, situations)
            update_from_taxonomy([data.pop('serviceTypeName')], responses, situations)
            update_from_taxonomy((data.pop('serviceTargetAudience') or '').split(';'), responses, situations)
            update_from_taxonomy(['soproc:' + relatedMalkarService.get('serviceGovId', '')], responses, situations)

            payment_required = data.pop('paymentMethod')
            if payment_required in ('Free service', None):
                row['payment_required'] = 'no'
                row['payment_details'] = None
            elif payment_required == 'Symbolic cost':
                row['payment_required'] = 'yes'
                row['payment_details'] = 'עלות סמלית'
            elif payment_required == 'Full payment':
                row['payment_required'] = 'yes'
                row['payment_details'] = 'השירות ניתן בתשלום'
            elif payment_required == 'Government funded':
                row['payment_required'] = 'yes'
                row['payment_details'] = 'השירות מסובסד על ידי הממשלה'
            else:
                assert False, payment_required + ' ' + repr(row)

            service_terms = data.pop('serviceTerms')
            if service_terms:
                if row.get('payment_details'):
                    row['payment_details'] += ', ' + service_terms
                else:
                    row['payment_details'] = service_terms

            details = []
            areas = []
            national = False

            area = (data.pop('area') or '').split(';')
            for item in area:
                if item == 'In Branches':
                    areas.append('בסניפי הארגון')
                    if len(row['branches']) == 0:
                        row['branches'] = ['guidestar:' + bid for bid in actual_branch_ids]
                elif item == 'Country wide':
                    areas.append('בתיאום מראש ברחבי הארץ')
                    national = True
                elif item == 'Customer Place':
                    areas.append('בבית הלקוח')
                    national = True
                elif item == 'Remote Service':
                    areas.append('שירות מרחוק')
                    national = True
                elif item == 'Via Phone or Mail':
                    areas.append('במענה טלפוני, צ׳אט או בדוא"ל')
                    national = True
                elif item == 'Web Service':
                    areas.append('בשירות אינטרנטי מקוון')
                    national = True
                elif item == 'Customer Appointment':
                    areas.append('במפגשים קבוצתיים או אישיים')
                    national = True
                elif item == 'Program':
                    areas.append('תוכנית ייעודית בהרשמה מראש')
                    national = True
                elif item in ('Not relevant', ''):
                    pass
                else:
                    assert False, 'area {}: {!r}'.format(area, row)

            if len(areas) > 1:
                details.append('השירות ניתן: ' + ', '.join(areas))
            elif len(areas) == 1:
                details.append('השירות ניתן ' + ''.join(areas))

            if national:
                row['branches'].append(f'guidestar:{orgId}:national')
            # if len(row['branches']) == 0:
            #     continue

            when = data.pop('whenServiceActive')
            if when == 'All Year':
                details.append('השירות ניתן בכל השנה')
            elif when == 'Requires Signup':
                details.append('השירות ניתן בהרשמה מראש')
            elif when == 'Time Limited':
                details.append('השירות מתקיים בתקופה מוגבלת')
            elif when == 'Criteria Based':
                details.append('השירות ניתן על פי תנאים או קריטריונים')
            elif when is None:
                pass
            else:
                assert False, 'when {}: {!r}'.format(when, row)

            remoteDelivery = (data.pop('remoteServiceDelivery') or '').split(';')
            # Phone, Chat / Email / Whatsapp, Internet, Zoom / Hybrid, Other
            methods = []
            for item in remoteDelivery:
                if item == 'Phone':
                    methods.append('טלפון')
                elif item == 'Chat / Email / Whatsapp':
                    methods.append('בצ׳אט, דוא"ל או וואטסאפ')
                elif item == 'Internet':
                    methods.append('אתר אינטרנט')
                elif item == 'Zoom / Hybrid':
                    methods.append('בשיחת זום')
                elif item == '':
                    pass
                elif item == 'Other':
                    pass
                else:
                    assert False, 'remoteDelivery {!r}: {!r}'.format(item, remoteDelivery)

            remoteDeliveryOther = data.pop('RemoteServiceDelivery_Other')
            if remoteDeliveryOther:
                methods.append(remoteDeliveryOther)

            if len(methods) > 0:
                details.append('שירות מרחוק באמצעות: ' + ', '.join(methods))

            if relatedMalkarService:
                relatedId = relatedMalkarService.get('serviceGovId')
                relatedOffice = relatedMalkarService.get('serviceOffice')
                print('GOT RELATED: id={}, office={}'.format(relatedId, relatedOffice))
                if relatedId and relatedOffice:
                    row['implements'] = f'soproc:{relatedId}#{relatedOffice}'

            startDate = data.pop('startDate', None)
            endDate = data.pop('endDate', None)
            if startDate:
                startDate = datetime.datetime.fromisoformat(startDate[:19]).date().strftime('%d/%m/%Y')
                details.append('תאריך התחלה: ' + startDate)
            if endDate:
                endDate = datetime.datetime.fromisoformat(endDate[:19]).date().strftime('%d/%m/%Y')
                details.append('תאריך סיום: ' + endDate)

            row['details'] = '\n<br/>\n'.join(details)
            url = data.pop('url')
            url = fix_url(url)
            if url:
                row['urls'] = f'{url}#מידע נוסף על השירות'

            phone_numbers = data.pop('Phone', data.pop('phone', None))
            if phone_numbers:
                row['phone_numbers'] = phone_numbers

            email_address = data.pop('Email', data.pop('email', None))
            if email_address:
                row['email_address'] = email_address

            for k in ('isForCoronaVirus', 'lastModifiedDate', 'serviceId', 'isForBranch'):
                data.pop(k)
            row['situations'] = sorted(situations)
            row['responses'] = sorted(responses)
            assert all(v in (None, '0') for v in data.values()), repr(data_source_url) + ':' + repr(data)
            count += 1
            yield row

        print('DONE EMITTING SERVICES', count,'/',full_count)

    return DF.Flow(
        func,
    )


def fetchServiceData(ga, taxonomy):
    print('FETCHING ALL ORGANIZATION SERVICES')

    airtable_updater(settings.AIRTABLE_SERVICE_TABLE, 'guidestar',
        ['name', 'description', 'details', 'payment_required', 'payment_details', 'urls', 'situations', 'responses', 
        'organizations', 'branches', 'data_sources', 'implements', 'phone_numbers', 'email_address'],
        DF.Flow(
            load_from_airtable(settings.AIRTABLE_DATA_IMPORT_BASE, settings.AIRTABLE_ORGANIZATION_TABLE, settings.AIRTABLE_VIEW, settings.AIRTABLE_API_KEY),
            DF.update_resource(-1, name='orgs'),
            DF.filter_rows(lambda r: r['status'] == 'ACTIVE', resources='orgs'),
            DF.select_fields(['id', 'name', 'source'], resources='orgs'),
            unwind_services(ga),
            # DF.checkpoint('unwind_services'),
        ),
        DF.Flow(
            updateServiceFromSourceData(taxonomy),
            # lambda rows: (r for r in rows if 'drop' in r), 
        ),
        airtable_base=settings.AIRTABLE_DATA_IMPORT_BASE
    )


def getGuidestarOrgs(ga: GuidestarAPI):
    today = datetime.date.today().isoformat()
    regNums = [
        dict(id=org['id'], data=dict(id=org['id'], last_tag_date=today))
        for org in ga.organizations()
    ]

    print('COLLECTED {} guidestar organizations'.format(len(regNums)))
    airtable_updater(settings.AIRTABLE_ORGANIZATION_TABLE, 'entities',
        ['last_tag_date'],
        regNums, update_mapper(), 
        manage_status=False,
        airtable_base=settings.AIRTABLE_DATA_IMPORT_BASE
    )


def operator(name, params, pipeline):
    logger.info('STARTING Entity + Guidestar Scraping')

    taxonomy = dict()
    print('FETCHING TAXONOMY MAPPING')
    taxonomy = DF.Flow(
        load_from_airtable(settings.AIRTABLE_BASE, settings.AIRTABLE_TAXONOMY_MAPPING_GUIDESTAR_TABLE, settings.AIRTABLE_VIEW, settings.AIRTABLE_API_KEY),
        # DF.printer(),
        DF.select_fields(['name', 'situation_ids', 'response_ids']),
    ).results()[0][0]
    taxonomy = dict(
        (r.pop('name'), r) for r in taxonomy
    )

    print('FETCHING SOPROC MAPPING')
    soproc_mappings = DF.Flow(
        load_from_airtable(settings.AIRTABLE_DATA_IMPORT_BASE, settings.AIRTABLE_TAXONOMY_MAPPING_SOPROC_TABLE, settings.AIRTABLE_VIEW, settings.AIRTABLE_API_KEY),
        DF.select_fields(['id', 'situation_ids', 'response_ids']),
    ).results()[0][0]
    taxonomy.update(dict(
        (r.pop('id'), r) for r in soproc_mappings
    ))

    ga = GuidestarAPI()
    ga.fetchCaches()
    getGuidestarOrgs(ga)
    fetchOrgData(ga)
    fetchBranchData(ga)
    fetchServiceData(ga, taxonomy)


if __name__ == '__main__':
    operator(None, None, None)
