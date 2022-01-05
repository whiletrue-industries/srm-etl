import datetime
import tempfile

import dataflows as DF
from dataflows_ckan import dump_to_ckan
from datapackage import Package

from conf import settings
from srm_tools.logger import logger

def data_api_sitemap_flow():
    urls = DF.Flow(
        [dict(path='/')],
        DF.load(f'{settings.DATA_DUMP_DIR}/response_data/datapackage.json'),
        DF.load(f'{settings.DATA_DUMP_DIR}/place_data/datapackage.json'),
        DF.load(f'{settings.DATA_DUMP_DIR}/card_data/datapackage.json'),
        DF.add_field('path', 'string', lambda r: '/r/{id}'.format(**r), resources='responses'),
        DF.add_field('path', 'string', lambda r: '/p/{key}'.format(**r), resources='places'),
        DF.add_field('path', 'string', lambda r: '/c/{card_id}'.format(**r), resources='card_data'),
        DF.concatenate(dict(path=[]), target=dict(name='sitemap', path='sitemap.csv')),
        DF.set_type('path', transform=lambda v: v.replace("'", '&apos;').replace('"', '&quot;')),
        DF.printer()
    ).results()[0][0]
    today = datetime.date.today().isoformat()
    with tempfile.NamedTemporaryFile(mode='w') as buff:
        buff.write('<?xml version="1.0" encoding="UTF-8"?><urlset xmlns="http://www.sitemaps.org/schemas/sitemap/0.9">')
        for row in urls:
            buff.write('<url><loc>https://www.kolsherut.org.il{}</loc><lastmod>{}</lastmod></url>\n'.format(row['path'], today))
        buff.write('</urlset>')
        dumper = dump_to_ckan(settings.CKAN_HOST, settings.CKAN_API_KEY, settings.CKAN_OWNER_ORG, force_format=False)
        datapackage = dict(
            name='sitemap',
            resources=[dict(
                name='sitemap',
                path='sitemap.xml',
                format='xml',
                schema=dict(
                    fields=[dict(name='path', type='string')]
                )
            )],
        )
        dumper.datapackage = Package(datapackage)
        dumper.write_ckan_dataset(dumper.datapackage)
        buff.flush()
        print(dumper.datapackage.resources[0].descriptor)
        dumper.write_file_to_output(buff.name, 'sitemap.xml')


def operator(*_):
    logger.info('Starting Sitemap Flow')

    # relational_sql_flow().process()
    data_api_sitemap_flow()

    logger.info('Finished SQL Flow')


if __name__ == '__main__':
    operator(None, None, None)
