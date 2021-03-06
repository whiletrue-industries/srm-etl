from srm_tools.budgetkey import fetch_from_budgetkey
import dataflows as DF
from dataflows.base.resource_wrapper import ResourceWrapper

from dataflows_airtable import dump_to_airtable, load_from_airtable
from dataflows_airtable.consts import AIRTABLE_ID_FIELD

from srm_tools.update_table import airtable_updater_flow, airtable_updater
from srm_tools.situations import Situations
from srm_tools.guidestar_api import GuidestarAPI

from conf import settings
from srm_tools.logger import logger


situations = Situations()


## ORGANIZATIONS
def fetchEntityFromBudgetKey(regNum):
    entity = list(fetch_from_budgetkey(f"select * from entities where id='{regNum}'"))
    if regNum == '513847251':
        print('513847251513847251', entity)
    if len(entity) > 0:
        entity = entity[0]
        name = entity['name']
        for x in (
            'בעמ',
            'בע״מ',
            "בע'מ",
            'ע״ר',
            'חל״צ'
        ):
            name = name.replace(x, '')
        name = name.strip()
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
    def func(row):
        regNums = [row['id']]
        for data in ga.organizations(regNums=regNums):
            try:
                data = data['data']
                row['name'] = data['name'].replace(' (חל"צ)', '').replace(' (ע"ר)', '')
                row['kind'] = data['malkarType']
                row['description'] = None
                row['purpose'] = data.get('orgGoal')
                urls = []
                if data.get('website'):
                    urls.append(data['website'] + '#אתר הבית')
                if data.get('urlGuidestar'):
                    urls.append(data['urlGuidestar'] + '#מידע נוסף ב״גיידסטאר״')
                row['urls'] = '\n'.join(urls)
                break
            except Exception as e:
                print('BAD DATA RECEIVED', str(e), regNums, data)
        else:
            data = fetchEntityFromBudgetKey(row['id'])
            if data is not None:
                row.update(data['data'])
            else:
                print('NOT FOUND', regNums)
    return func

def fetchOrgData(ga):
    regNums = DF.Flow(
        load_from_airtable(settings.AIRTABLE_BASE, settings.AIRTABLE_ORGANIZATION_TABLE, settings.AIRTABLE_VIEW, settings.AIRTABLE_API_KEY),
        DF.filter_rows(lambda row: row.get('source') == 'entities'),
        DF.select_fields(['id']),
        DF.add_field('data', 'object', dict(name='')),
    ).results()[0][0]

    print('COLLECTED {} relevant organizations'.format(len(regNums)))
    airtable_updater(settings.AIRTABLE_ORGANIZATION_TABLE, 'entities',
        ['name', 'kind', 'urls', 'description', 'purpose'],
        regNums,
        updateOrgFromSourceData(ga),
    )


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
                        data['name'] = (ret['short_name'] or ret['name']) + ' - ' + branch['cityName']
                    data['address'] = calc_location_key(branch)
                    data['location'] = [data['address']]
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
                        data['situations'] = situations.convert_situation_list([
                            'human_situations:language:{}_speaking'.format(l.lower().strip()) for l in branch['language'].split(';')
                        ])

                    ret['data'] = data
                    ret['id'] = 'guidestar:' + branch['branchId']
                    yield ret
                if len(branches) == 0:
                    print('FETCHING FROM BUDGETKEY', regNum, branches)
                    if row['kind'] not in ('עמותה', 'חל"צ'):
                        ret = dict()
                        ret.update(row)
                        name = row['name']
                        ret.update(dict(
                            id='budgetkey:' + regNum,
                            data=dict(
                                name=name,
                                address=name,
                                organization=[regNum]
                            )
                        ))
                        yield ret
    return DF.Flow(
        DF.add_field('data', 'object', resources='orgs'),
        func,
    )

def calc_location_key(row):
    key = ''
    cityName = row.get('cityName')
    if cityName:
        streetName = row.get('streetName')
        if streetName:
            key += f'{streetName} '
            houseNum = row.get('houseNum')
            if houseNum:
                key += f'{houseNum} '
            key += ', '
        key += f'{cityName} '
    
    alternateAddress = row.get('alternateAddress')
    if alternateAddress:
        if alternateAddress not in key:
            key += f' - {alternateAddress}'
    key = key.strip()

    return key or None

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
        airtable_updater_flow(settings.AIRTABLE_BRANCH_TABLE, 'entities',
            ['name', 'organization', 'address', 'address_details', 'location', 'description', 'phone_numbers', 'urls', 'situations'],
            DF.Flow(
                load_from_airtable(settings.AIRTABLE_BASE, settings.AIRTABLE_ORGANIZATION_TABLE, settings.AIRTABLE_VIEW),
                DF.update_resource(-1, name='orgs'),
                DF.filter_rows(lambda r: r['source'] == 'entities', resources='orgs'),
                DF.filter_rows(lambda r: r['status'] == 'ACTIVE', resources='orgs'),
                DF.select_fields(['id', 'name', 'short_name', 'kind'], resources='orgs'),
                unwind_branches(ga),
            ),
            updateBranchFromSourceData(),
        )
    ).process()


def operator(name, params, pipeline):
    logger.info('STARTING Entity Scraping')
    ga = GuidestarAPI()

    fetchOrgData(ga)
    fetchBranchData(ga)


if __name__ == '__main__':
    operator(None, None, None)
