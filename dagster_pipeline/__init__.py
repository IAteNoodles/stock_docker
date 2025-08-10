from dagster import Definitions, load_assets_from_modules, define_asset_job

from . import assets
from .schedules import daily_update_schedule, stock_data_update_daily_job
from .sensors import trackfile_change_sensor

all_assets = load_assets_from_modules([assets])

# Inline job used previously (still exported for ad-hoc runs)
stock_data_update_job = define_asset_job(
    name="stock_data_update_job",
    selection=[assets.update_stock_data],
)

defs = Definitions(
    assets=all_assets,
    jobs=[stock_data_update_job, stock_data_update_daily_job],
    schedules=[daily_update_schedule],
    sensors=[trackfile_change_sensor],
)
