import dataflows as DF
from dataflows.helpers.resource_matcher import ResourceMatcher
from dataflows_airtable import load_from_airtable, dump_to_airtable, AIRTABLE_ID_FIELD
from dataflows_ckan import dump_to_ckan

from conf import settings

class Report():

    def __init__(self, name, slug, fields, id_fields):
        self.name = name
        self.slug = slug
        self.fields = fields
        self.id_fields = id_fields
        self.added = set()
        self.records = []

    def add(self, rec):
        key = tuple(rec[f] for f in self.id_fields)
        if key in self.added:
            return
        self.added.add(key)
        self.records.append(dict((f, rec.get(f)) for f in self.fields))

    def save(self):
        DF.Flow(
            self.records,
            DF.update_package(title=self.name, name=self.slug),
            DF.update_resource(-1, name='report', path='report.csv'),
        #     dump_to_ckan(
        #         settings.CKAN_HOST,
        #         settings.CKAN_API_KEY,
        #         settings.CKAN_OWNER_ORG,
        #     )
         ).process()


class Stats():

    def __init__(self):
        self.load()
        self.dirty = {}

    def load(self):
        self.data = DF.Flow(
            load_from_airtable(settings.AIRTABLE_BASE, settings.AIRTABLE_STATS_TABLE, settings.AIRTABLE_VIEW, settings.AIRTABLE_API_KEY),
            DF.filter_rows(lambda row: row.get('name') is not None),
        ).results()[0][0]
        self.data = {row['name']: row for row in self.data}

    def set_stat(self, stat, value):
        rec = {
            'name': stat,
            'value': value
        }
        if stat in self.data:
            rec[AIRTABLE_ID_FIELD] = self.data[stat][AIRTABLE_ID_FIELD]
        DF.Flow(
            [rec],
            DF.update_resource(-1, name=settings.AIRTABLE_STATS_TABLE),
            dump_to_airtable({
                (settings.AIRTABLE_BASE, settings.AIRTABLE_STATS_TABLE): {
                    'resource-name': settings.AIRTABLE_STATS_TABLE,
                }
            }, apikey=settings.AIRTABLE_API_KEY)
        ).process()
        if AIRTABLE_ID_FIELD not in rec:
            self.load()
        else:
            self.data[stat] = rec

    def increase(self, stat):
        current = 0
        if stat in self.dirty:
            current = self.dirty[stat]
        self.dirty[stat] = current + 1

    def save(self):
        for stat, value in self.dirty.items():
            self.set_stat(stat, value)
        self.dirty = {}

    def filter_with_stat(self, stat: str, filter_func, passing=False, resources=None, report: Report=None):

        def process_resource(rows):
            count = 0
            for row in rows:
                if filter_func(row):
                    if passing:
                        if report:
                            report.add(row)
                        count += 1
                    yield row
                else:
                    if not passing:
                        if report:
                            report.add(row)
                        count += 1
            self.set_stat(stat, count)
            if report:
                report.save()

        def func(package: DF.PackageWrapper):
            matcher = ResourceMatcher(resources, package.pkg)
            yield package.pkg
            for r in package:
                if matcher.match(r.res.name):
                    yield process_resource(r)
                else:
                    yield r

        return func
