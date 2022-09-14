# doltbounty-housing-wake-county
Dolt bounty data scraper to download house pricing information for house sales in Wake County, NC one-by-one.

## Setup
1. Install requirements.txt with `python3 -m pip install -r requirements.txt`
2. Download/extract `us/nc/wake` > `us_nc_wake-addresses-county.geojson` from https://batch.openaddresses.io/data in this folder. You can commit it in the future, or not. It is not currently ignored by the `.gitignore`.

## Notes
* Ignore the work with parcels. It ended up being unused.
* The Step 2 notebook was for development/testing, and the Step 2 script was what to use in the end.
