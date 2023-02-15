import pandas as pd
import geopandas as gpd
import numpy as np

ALLPARCELS = None
DBIPERMITS = None
rhna4start = 2007
rhna4end = 2015


def get_parcels():
    """Return df of all parcels.
    """
    global ALLPARCELS 
    if ALLPARCELS is None:
        ALLPARCELS = gpd.read_file('./data/Parcels   Active and Retired/parcels.shp')
    return ALLPARCELS

def get_dbi_permits():
    """Return df of all parcels.
    """
    global DBIPERMITS 
    if DBIPERMITS is None:
        DBIPERMITS = gpd.read_file('./data/Building Permits.geojson')
    return DBIPERMITS

def clean_dates(df):
    """In-place conversion of dates"""
    date_cols = [c for c in df.columns if 'date' in c.lower()]
    for date in date_cols:
        df[date] = pd.to_datetime(df[date], errors='coerce')
        

def get_rhna4_parcels(df):
    """Remove parcels deleted before RHNA4 or added after RHNA4."""
    clean_dates(df)
    df = df[~(df.date_map_d.dt.year < rhna4start)]
    df = df[~(df.date_rec_d.dt.year < rhna4start)]
    df = df[~(df.date_rec_a.dt.year >= rhna4end)]
    df = df[~(df.date_map_a.dt.year >= rhna4end)]
    return df

def transform_bluesky_to_geospatial(bluesky):
    """Return a geodataframe from bluesky. Adds columns for mapblklot, blklot,
    geometry, CANTID_blklot_backup, CANTID_geometry_backup
    """
    
    allParcels = get_parcels() 
    # TODO: Change for when i do rhna5 
    allParcels = get_rhna4_parcels(allParcels)
    geoDf = allParcels.merge(bluesky, right_on='MapBlkLot_Master', left_on='mapblklot', how='right')

    # For each MapBlkLot_Master, get a list of unique blklot and geometry fields.
    cantIDBackup = geoDf.groupby('MapBlkLot_Master')[['blklot', 'geometry']].agg(['unique'])
    cantIDBackup = cantIDBackup.reset_index()
    cantIDBackup = cantIDBackup.droplevel(1, axis=1)

    # When list of unique blklot and geometry is empty or has a single element, replace list with nan.
    def backup(orig):
        if len(orig) <= 1:
            return np.nan
        return orig

    # Clean cantIDBackUp
    cantIDBackup.blklot = cantIDBackup.blklot.apply(backup)
    cantIDBackup.geometry = cantIDBackup.geometry.apply(backup)
    cantIDBackup = cantIDBackup.rename({'blklot': 'CANTID_blklot_backup',
                                        'geometry': 'CANTID_geometry_backup'}, axis=1)
    
    # For now, let's roll with most recent parcel added to map. If it fails to yield permit match, we can use
    # the parcels saved in cantIDBackup.
    geoDf = geoDf.sort_values('date_map_a', ascending=False).groupby('MapBlkLot_Master').nth(0)
    geoDf = geoDf.drop(allParcels.columns[2:-2], axis=1)
    geoDf = geoDf.reset_index()
    geoDf = geoDf.merge(cantIDBackup, right_on='MapBlkLot_Master', left_on='MapBlkLot_Master', how='right')
    return geoDf

def get_rhna_permits():
    """Get permits that count towards RHNA from DBI Permits."""
    dbi = get_dbi_permits()
    clean_dates(dbi)
    dbi['units'] = dbi.proposed_units.fillna(0).astype(float) - dbi.existing_units.fillna(0).astype(float)
    dbi['blocklot'] = dbi['block'].astype(str) + dbi['lot'].astype(str)
    dbi['permit_type'] = dbi['permit_type'].astype(int)
    dbi['na_existing_units'] = dbi['existing_units'].isna()
    relevant_uses = [
        'apartments', '1 family dwelling', '2 family dwelling',
        'residential hotel', 'misc group residns.', 'artist live/work',
        'convalescent home', 'accessory cottage', 'nursing home non amb',
        'orphanage', 'r-3(dwg) nursing', 'nursing home gt 6', 
        'day care home gt 12', 'day care home lt 7', 'day care home 7 - 12',
        'nursing home lte 6'
    ]

    # In old code, there may have been two bugs. One, I didn't caste permit type as int first. Second, & hass higher precedence
    # so first line needs parentheses
    rhna_permits = dbi[
        (dbi['units'] > 0)
        & (dbi['proposed_use'].isin(relevant_uses))
        & (dbi['permit_type'].isin([1, 2, 3, 8]))
    ].copy()
    
    # Let's try without the below
    # rhna_permits.query('not (`permit_type` == 8 and na_existing_units)', inplace=True)
    # rhna_permits.query('not (`permit_type` == 3 and na_existing_units)', inplace=True)

    rhna_permits = rhna_permits.rename(columns={'Location': 'geometry'})
    
    rhna_permits = rhna_permits[
        ((rhna_permits.status_date.dt.year >= rhna4start)
         & (rhna_permits.status_date.dt.year < rhna4end))
        | ((rhna_permits.permit_creation_date.dt.year >= rhna4start)
           & (rhna_permits.permit_creation_date.dt.year < rhna4end))
        | ((rhna_permits.issued_date.dt.year >= rhna4start)
           & (rhna_permits.issued_date.dt.year < rhna4end))
        | ((rhna_permits.filed_date.dt.year >= rhna4start)
           & (rhna_permits.filed_date.dt.year < rhna4end))
        | ((rhna_permits.completed_date.dt.year >= rhna4start)
           & (rhna_permits.completed_date.dt.year < rhna4end))]
    return rhna_permits