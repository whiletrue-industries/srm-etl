from . import to_dp, to_es, to_mapbox, to_sitemap

from srm_tools.logger import logger


def operator(*_):

    logger.info('Starting Derive Data Flow')

    to_dp.operator()
    to_es.operator()
    to_mapbox.operator()
    to_sitemap.operator()
    # to_sql.operator()

    logger.info('Finished Derive Data Flow')


if __name__ == '__main__':

    operator(None, None, None)
