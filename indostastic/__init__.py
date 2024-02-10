from dagster import AssetSelection, Definitions, define_asset_job, load_assets_from_modules

from . import assets

all_assets = load_assets_from_modules([assets])

neww_job = define_asset_job("neww_job", selection=AssetSelection.all())

defs = Definitions(
    assets=all_assets,
    jobs=[neww_job],
)