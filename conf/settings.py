from dotenv import load_dotenv
from .utils import get_env, EnvVarStrategy as s


load_dotenv()


CLICK_API = get_env("ETL_CLICK_API")

GUIDESTAR_API = get_env("ETL_GUIDESTAR_API")
GUIDESTAR_USERNAME = get_env("ETL_GUIDESTAR_USERNAME")
GUIDESTAR_PASSWORD = get_env("ETL_GUIDESTAR_PASSWORD")

GOVMAP_GEOCODE_API = get_env("ETL_GOVMAP_GEOCODE_API")
GOVMAP_GEOCODE_AUTH = get_env("ETL_GOVMAP_GEOCODE_AUTH")
GOVMAP_GEOCODE_API_ENTRYPOINT = get_env("ETL_GOVMAP_GEOCODE_API_ENTRYPOINT")

API_REQUEST_ORIGIN = get_env("ETL_API_REQUEST_ORIGIN")

AIRTABLE_VIEW = get_env("ETL_AIRTABLE_VIEW")
AIRTABLE_BASE = get_env("ETL_AIRTABLE_BASE")
AIRTABLE_LOCATION_TABLE = get_env("ETL_AIRTABLE_LOCATION_TABLE")
AIRTABLE_ORGANIZATION_TABLE = get_env("ETL_AIRTABLE_ORGANIZATION_TABLE")
AIRTABLE_BRANCH_TABLE = get_env("ETL_AIRTABLE_BRANCH_TABLE")
AIRTABLE_SITUATION_TABLE = get_env("ETL_AIRTABLE_SITUATION_TABLE")

MAPBOX_ACCESS_TOKEN = get_env("ETL_MAPBOX_ACCESS_TOKEN")
MAPBOX_LIST_TILESETS = get_env("ETL_MAPBOX_LIST_TILESETS")
MAPBOX_UPLOAD_CREDENTIALS = get_env("ETL_MAPBOX_UPLOAD_CREDENTIALS")
MAPBOX_CREATE_UPLOAD = get_env("ETL_MAPBOX_CREATE_UPLOAD")
MAPBOX_UPLOAD_STATUS = get_env("ETL_MAPBOX_UPLOAD_STATUS")
