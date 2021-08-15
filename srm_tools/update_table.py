import dataflows as DF

from dataflows_airtable import dump_to_airtable, load_from_airtable
from dataflows_airtable.consts import AIRTABLE_ID_FIELD
from conf import settings


def airflow_table_updater(
    table, source_id, table_fields, fetch_data_flow, update_data_flow
):
    """
    Updates the given airflow table with new data, maintaining status correctly.
    :param table: The table to update.
    :param source_id: The source value to check for and use for new data.
    :param table_fields: The fields in the airflow table that we want to update (except the id, source and status fields).
    :param fetch_data_flow: Flow which will be used to fetch new data.
        Needs to add a new resource with two fields - 'id' (unique row id) and 'data' (object with the newly fetched data).
    :param update_data_flow: Flow to use to map the 'data' field into the table standard fields.
    """
    DF.Flow(
        airflow_table_update_flow(
            table, source_id, table_fields, fetch_data_flow, update_data_flow
        ),
        DF.printer(),
    ).process()


def airflow_table_update_flow(
    table, source_id, table_fields, fetch_data_flow, update_data_flow
):
    """
    Updates the given airflow table with new data, maintaining status correctly.
    :param table: The table to update.
    :param source_id: The source value to check for and use for new data.
    :param table_fields: The fields in the airflow table that we want to update (except the id, source and status fields).
    :param fetch_data_flow: Flow which will be used to fetch new data.
        Needs to add a new resource with two fields - 'id' (unique row id) and 'data' (object with the newly fetched data).
    :param update_data_flow: Flow to use to map the 'data' field into the table standard fields.
    """
    return DF.Flow(
        load_from_airtable(settings.AIRTABLE_BASE, table, settings.AIRTABLE_VIEW),
        DF.update_resource(-1, name="current"),
        DF.filter_rows(lambda r: r["source"] == source_id, resources="current"),
        fetch_data_flow,
        DF.update_resource(-1, name="fetched"),
        DF.join(
            "current",
            ["id"],
            "fetched",
            ["id"],
            dict((f, None) for f in [*table_fields, AIRTABLE_ID_FIELD]),
            mode="full-outer",
        ),
        DF.add_field(
            "status",
            "string",
            lambda r: "ACTIVE" if r.get("data") else "INACTIVE",
            resources="fetched",
        ),
        DF.add_field("source", "string", source_id, resources="fetched"),
        update_data_flow,
        DF.select_fields(
            ["id", "source", "status", *table_fields, AIRTABLE_ID_FIELD],
            resources="fetched",
        ),
        dump_to_airtable(
            {
                (settings.AIRTABLE_BASE, table): {
                    "resource-name": "fetched",
                    "typecast": True,
                }
            }
        ),
    )
