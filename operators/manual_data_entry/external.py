import shutil

import dataflows as DF
import dataflows_airtable as DFA

from conf import settings
from srm_tools.stats import Stats

from mde_utils import load_manual_data


CHECKPOINT = 'external-mde'

def filter_ready_to_publish(stats: Stats):
    def func(rows):
        for row in rows:
            if row.get('סטטוס') == 'מוכן לפרסום':
                yield row
            else:
                stats.increase('External Manual Data: Entry not ready to publish')
    return func

def fetch_google_spreadsheet(stats):
    def func(rows):
        for row in rows:
            URL = row.get('Google Spreadsheet')
            if URL:
                print('Loading data from', URL, '...')
                services = DF.Flow(
                    DF.load(URL, headers=2, deduplicate_headers=True),
                    DF.filter_rows(lambda r: bool(r['שם השירות'])),
                    filter_ready_to_publish(stats),
                    # DF.printer(),
                ).results()[0][0]
                for i, service in enumerate(services):
                    emit = dict()
                    emit['_row_id'] = f'{URL}#{i}'
                    emit['Status'] = 'בייצור'
                    emit['Branch Address'] = service['כתובת או שם ישוב בו מסופק השירות'] or row.get('Branch Address')
                    emit['Branch Details'] = None
                    emit['Branch Geocode'] = None
                    emit['Branch Email'] = None
                    emit['Branch Website'] = None
                    emit['Branch Phone Number'] = None
                    emit['Service Name'] = service['שם השירות']
                    emit['Service Description'] = service['תיאור השירות']
                    emit['Service Conditions'] = service['אופן קבלת השירות']
                    emit['Service Phone Number'] = service['מספר טלפון תקני (1)']
                    emit['Service Website'] = service['דף אינטרנט']
                    emit['Service Email'] = service['אימייל (1)']
                    if service['שם המפעיל']:
                        emit['Org Name'] = service['שם המפעיל']
                        emit['Org Short Name'] = None
                        emit['Org Id'] = service['מספר תאגיד']
                        if not emit['Org Id']:
                            continue
                        if emit['Org Id'].strip() == 'יוזמה פרטית':
                            emit['Org Id'] = None
                        emit['Org Phone Number'] = service['מספר טלפון תקני (2)']
                        emit['Org Email'] = service['אימייל (2)']
                        emit['Org Website'] = service['אתר אינטרנט']
                    else:
                        emit['Org Name'] = row['Org Name']
                        emit['Org Short Name'] = row.get('Org Short Name')
                        emit['Org Id'] = row['Org Id']
                        emit['Org Phone Number'] = row.get('Org Phone Number')
                        emit['Org Email'] = row.get('Org Email')
                        emit['Org Website'] = row.get('Org Website')
                    emit['Data Source'] = row['Source Name']
                    try:
                        emit['taxonomies'] = [service['קטגוריה'], service['אוכלוסיית יעד'], service['שפה'], service.get('שפה-2'), service.get('שפה-3'), service.get('שפה-4'), service.get('שפה-5')]
                    except KeyError:
                        pass
                    try:
                        emit['target_audiences'] = service['אוכלוסיות יעד']
                    except KeyError:
                        emit['target_audiences'] = None
                    emergency_service_msg = 'יש לתייג כשירות חירום'
                    emergency_service = False
                    try:
                        emergency_service = service['שירות למצב החירום'] is True
                    except KeyError:
                        pass
                    try:
                        notes = service['הערות חופשיות'] or ''
                        notes = (notes + '\n\n' + emergency_service_msg) if emergency_service else notes
                        notes = notes.strip()
                        emit['notes'] = notes
                    except KeyError:
                        emit['notes'] = None if not emergency_service else emergency_service_msg
                    yield emit

    return DF.Flow(
        DF.add_field('Branch Email', 'string'),
        DF.add_field('Branch Website', 'string'),
        DF.add_field('Branch Phone Number', 'string'),
        DF.add_field('Service Name', 'string'),
        DF.add_field('Service Description', 'string'),
        DF.add_field('Service Conditions', 'string'),
        DF.add_field('Service Phone Number', 'string'),
        DF.add_field('Service Website', 'string'),
        DF.add_field('Service Email', 'string'),        
        DF.add_field('Data Source', 'string'),
        DF.add_field('taxonomies', 'array'),
        DF.add_field('target_audiences', 'string'),
        DF.add_field('notes', 'string'),
        DF.add_field('_row_id', 'string'),
        func
    )

def handle_taxonomies(taxonomies):
    def func(row):
        responses = set()
        situations = set()
        if row.get('taxonomies'):
            for t in row['taxonomies']:
                if not t:
                    continue
                t = t.strip()
                if t in taxonomies:
                    responses.update(taxonomies[t]['response_ids'] or [])
                    situations.update(taxonomies[t]['situation_ids'] or [])
            row['responses_ids'] = list(responses)
            row['situations_ids'] = list(situations)

    return DF.Flow(
        DF.add_field('responses_ids', 'array'),
        DF.add_field('situations_ids', 'array'),
        func,
        DF.delete_fields(['taxonomies'])
    )


def main():
    data_sources = DF.Flow(
        DFA.load_from_airtable('app4yocYm963dR5Tt', 'Sheets', settings.AIRTABLE_VIEW, settings.AIRTABLE_API_KEY),
        DF.select_fields(['Source Name', 'Source Legalese'])
    ).results()[0][0]
    data_sources = dict(
        (r['Source Name'], r['Source Legalese'])
        for r in data_sources
    )
    print(data_sources)
    taxonomies = DF.Flow(
        DFA.load_from_airtable('app4yocYm963dR5Tt', 'Categories', settings.AIRTABLE_VIEW, settings.AIRTABLE_API_KEY),
        DF.select_fields(['name', 'response_ids', 'situation_ids'])
    ).results()[0][0]
    taxonomies = dict(
        (r.pop('name').strip(), r)
        for r in taxonomies
    )
    print(taxonomies)

    shutil.rmtree(f'.checkpoints/{CHECKPOINT}', ignore_errors=True, onerror=None)

    stats = Stats()
    DF.Flow(
        DFA.load_from_airtable('app4yocYm963dR5Tt', 'Sheets', settings.AIRTABLE_VIEW, settings.AIRTABLE_API_KEY),
        stats.filter_with_stat('External Manual Data: Sheet not ready to publish', lambda r: r['Status'] == 'בייצור'),
        fetch_google_spreadsheet(stats),
        DF.delete_fields([DFA.AIRTABLE_ID_FIELD, 'Status', 'Google Spreadsheet', 'Source Legalese', 'Source Name']),
        handle_taxonomies(taxonomies),
        DF.printer(),
        DF.dump_to_path('test', format='xlsx'),
        DF.checkpoint(CHECKPOINT)
    ).process()
    stats.save()

    load_manual_data(DF.Flow(DF.checkpoint(CHECKPOINT)), data_sources, 'external-manual-data')


if __name__ == '__main__':
    main()
