import pandas as pd
import geopandas as gpd
import numpy as np

ALLPARCELS = None
DBIPERMITS = None
rhna3start = 1999
rhna4start = 2007
rhna5start = 2015

def get_parcels():
    """Return df of all parcels.
    """
    global ALLPARCELS 
    if ALLPARCELS is None:
        ALLPARCELS = gpd.read_file('../data/Parcels   Active and Retired/parcels.shp')
    return ALLPARCELS

def get_dbi_data():
    """Return df of all parcels.
    """
    global DBIPERMITS 
    if DBIPERMITS is None:
        DBIPERMITS = gpd.read_file('../data/Building Permits.geojson')
    return DBIPERMITS

def clean_dates(df):
    """In-place conversion of dates"""
    
    date_cols = [c for c in df.columns if 'date' in c.lower()]
    for date in date_cols:
        df[date] = pd.to_datetime(df[date], errors='coerce')
        
def clean_numbers(df):
    """In-place conversion of numbers where num is in column name"""
    num_cols = [c for c in df.columns if 'num' in c.lower()]
    for num in num_cols:
        df[num] = pd.to_numeric(df[num], errors='coerce')        

def get_rhna4_parcels(df):
    """Remove parcels deleted before RHNA4 or added after RHNA4."""
    clean_dates(df)
    df = df[~(df.date_map_d.dt.year < rhna4start)]
    df = df[~(df.date_rec_d.dt.year < rhna4start)]
    df = df[~(df.date_rec_a.dt.year >= (rhna4start + 8))]
    df = df[~(df.date_map_a.dt.year >= (rhna4start + 8))]
    return df.copy()

def get_rhna5_parcels(df):
    """Remove parcels deleted before RHNA4 or added after RHNA4."""
    clean_dates(df)
    df = df[~(df.date_map_d.dt.year < rhna5start)]
    df = df[~(df.date_rec_d.dt.year < rhna5start)]
    df = df[~(df.date_rec_a.dt.year >= (rhna5start + 8))]
    df = df[~(df.date_map_a.dt.year >= (rhna5start + 8))]
    return df.copy()

def get_site_inventory_feature(df, sites, cycle=4):
    rhna = sites[sites['rhnacyc'] == ('RHNA' + str(cycle))]
    rhna = rhna[rhna.jurisdict == 'San Francisco']

    rhna.locapn = rhna.locapn.str.split('/').str.join('')
    rhna.apn = rhna.apn.str.split('/').str.join('')

    predictedSites = np.logical_or(df.MapBlkLot_Master.isin(rhna.locapn.values),
                                   df.MapBlkLot_Master.isin(rhna.apn.values))
    df['inInventory'] = predictedSites
    return df

def merge_tax(df, tax, cycle=4, parcels=None):
    
    # Clean tax identifier
    def clean_apn(apn):
        apn = ''.join(str(apn).split(' '))
        return apn
    tax['MapBlkLot_Master'] = tax.RP1PRCLID.apply(clean_apn)
    
    # Merge on blocklot first
    full_df = df.merge(tax, how='inner', on='MapBlkLot_Master')
    
    # Get a geo version of the tax data. (This can be improved by using geojson on SF OpenData.)
    if parcels is None:
        parcels = get_parcels()
    allParcels = parcels
    #if cycle == 4:
    #    allParcels = get_rhna4_parcels(parcels)
    #else:
    #    allParcels = get_rhna5_parcels(parcels)
    allParcels2 = allParcels.dissolve(by='mapblklot')
    allParcels2 = allParcels2[['geometry']]
    taxGeo = allParcels2.merge(tax,
                               how='inner',
                               right_on='MapBlkLot_Master',
                               left_on='mapblklot',
                               validate='one_to_one')

    # Merge on geometry
    cantID = df[~df.MapBlkLot_Master.isin(tax.MapBlkLot_Master)]
    canID = gpd.sjoin(cantID, taxGeo, how="inner", predicate='intersects')
    canID = canID.drop_duplicates('MapBlkLot_Master_left')
    canID['MapBlkLot_Master'] = canID['MapBlkLot_Master_left']
    canID = canID.drop(['MapBlkLot_Master_left', 'MapBlkLot_Master_right', 'index_right'], axis=1)
    
    # Return concatenation of two merges
    return pd.concat((full_df, canID), axis=0)

def transform_bluesky_to_geospatial(bluesky, cycle=4):
    """Return a geodataframe from bluesky. Adds columns for mapblklot, blklot,
    geometry #, CANTID_blklot_backup, CANTID_geometry_backup
    """
    
    allParcels = get_parcels() 
    if cycle == 4:
        allParcels = get_rhna4_parcels(allParcels)
    elif cycle == 5:
        allParcels = get_rhna5_parcels(allParcels)
        
    geoDf = allParcels.merge(bluesky, right_on='MapBlkLot_Master', left_on='mapblklot', how='right')

    # For each MapBlkLot_Master, get a list of unique blklot and geometry fields.
    # cantIDBackup = geoDf.groupby('MapBlkLot_Master')[['blklot', 'geometry']].agg(['unique'])
    # cantIDBackup = cantIDBackup.reset_index()
    # cantIDBackup = cantIDBackup.droplevel(1, axis=1)

    # When list of unique blklot and geometry is empty or has a single element, replace list with nan.
    def backup(orig):
        if len(orig) <= 1:
            return np.nan
        return orig

    # Clean cantIDBackUp
    # cantIDBackup.blklot = cantIDBackup.blklot.apply(backup)
    # cantIDBackup.geometry = cantIDBackup.geometry.apply(backup)
    # cantIDBackup = cantIDBackup.rename({'blklot': 'CANTID_blklot_backup',
    #                                    'geometry': 'CANTID_geometry_backup'}, axis=1)
    
    # For now, let's roll with most recent parcel added to map. If it fails to yield permit match, we can use
    # the parcels saved in cantIDBackup.
    geoDf = geoDf.sort_values('date_map_a', ascending=False).groupby('MapBlkLot_Master').nth(0)
    geoDf = geoDf.drop(allParcels.columns[2:-2], axis=1)
    geoDf = geoDf.reset_index()
    #geoDf = geoDf.merge(cantIDBackup, right_on='MapBlkLot_Master', left_on='MapBlkLot_Master', how='right')
    return geoDf


def get_pipeline_permits(cycle=3, dbi=None):
    if dbi is None:
        dbi = get_dbi_data()

    clean_dates(dbi)
    if 'units' not in dbi.columns:
        dbi['units'] = dbi.proposed_units.fillna(0).astype(float) - dbi.existing_units.fillna(0).astype(float)
        dbi['blocklot'] = dbi['block'].astype(str) + dbi['lot'].astype(str)
        dbi['permit_type'] = dbi['permit_type'].astype(int)
        dbi['na_existing_units'] = dbi['existing_units'].isna()
    dbi = dbi.rename(columns={'Location': 'geometry'})
    dbi.estimated_cost = dbi.estimated_cost.astype(float)

    cycle_start = {3: rhna3start, 4: rhna4start, 5: rhna5start}
    start = cycle_start.get(cycle, 3)
    end = start + 8
    dbi = dbi[
        ((dbi.status_date.dt.year >= start)
         & (dbi.status_date.dt.year < end))
        | ((dbi.permit_creation_date.dt.year >= start)
           & (dbi.permit_creation_date.dt.year < end))
        | ((dbi.issued_date.dt.year >= start)
           & (dbi.issued_date.dt.year < end))
        | ((dbi.filed_date.dt.year >= start)
           & (dbi.filed_date.dt.year < end))
        | ((dbi.completed_date.dt.year >= start)
           & (dbi.completed_date.dt.year < end))].copy()

    # Group by mapblocklot, agg number of each permit type
    pipeline_by_type = dbi.groupby(['blocklot']).permit_type.value_counts().unstack()
    pipeline_by_type.columns = ['permit' + str(i) for i in pipeline_by_type.columns]
    pipeline_by_type.fillna(0, inplace=True)
    pipeline_by_cost = dbi.groupby('blocklot').estimated_cost.sum()

    # Group by mapblocklot, agg of permit cost
    pipeline_by_cost.name = 'permit_costs'
    pipeline = pd.concat((pipeline_by_cost, pipeline_by_type), axis=1)
    return pipeline

def get_dbi_permits(cycle=4, filter_for_construction=True):
    """Get permits that count towards RHNA from DBI Permits."""
    dbi = get_dbi_data()
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
    rhna_permits = dbi[
        (dbi['units'] > 0)
        & (dbi['proposed_use'].isin(relevant_uses))
        & (dbi['permit_type'].isin([1, 2, 3, 8]))
    ].copy()
    
    rhna_permits = rhna_permits.rename(columns={'Location': 'geometry'})
    
    cycle_start = {3: rhna3start, 4: rhna4start, 5: rhna5start}
    start = cycle_start.get(cycle, 4)
    end = start + 8
    rhna_permits = rhna_permits[
        ((rhna_permits.status_date.dt.year >= start)
         & (rhna_permits.status_date.dt.year < end))
        | ((rhna_permits.permit_creation_date.dt.year >= start)
           & (rhna_permits.permit_creation_date.dt.year < end))
        | ((rhna_permits.issued_date.dt.year >= start)
           & (rhna_permits.issued_date.dt.year < end))
        | ((rhna_permits.filed_date.dt.year >= start)
           & (rhna_permits.filed_date.dt.year < end))
        | ((rhna_permits.completed_date.dt.year >= start)
           & (rhna_permits.completed_date.dt.year < end))]
    return rhna_permits