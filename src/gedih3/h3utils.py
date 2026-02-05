import h3

def get_all_h3_hexagons(resolution: int):
    """Get all H3 hexagons at a given resolution level"""
    if resolution == 0:
        return list(h3.get_res0_cells())

    # For higher resolutions, start with res 0 and get all children
    all_hexagons = []
    base_cells = h3.get_res0_cells()

    for base_cell in base_cells:
        children = h3.cell_to_children(base_cell, resolution)
        all_hexagons.extend(children)

    return all_hexagons

def iter_all_h3_hexagons(resolution: int):
    """Memory-efficient iterator for all H3 hexagons at resolution"""
    base_cells = h3.get_res0_cells()

    for base_cell in base_cells:
        children = h3.cell_to_children(base_cell, resolution)
        for child in children:
            yield child

def fix_h3_geometry(hex:str):
    """Using the antimeridian package for robust handling."""
    from shapely.geometry import Polygon
    from antimeridian import fix_polygon
    boundary_coords = h3.cell_to_boundary(hex)    
    polygon = Polygon([(lon, lat) for lat, lon in boundary_coords])
    
    # Fix antimeridian crossing
    fixed_geometry = fix_polygon(polygon)
    return fixed_geometry

def intersect_h3_geometries(spatial, res=3, h3_ids=None):
    from shapely.geometry import box
    import geopandas as gpd
    if isinstance(spatial, list):
        spatial = box(*spatial)
    elif isinstance(spatial, gpd.GeoSeries) or isinstance(spatial, gpd.GeoDataFrame):
        spatial = spatial.to_crs(4326).union_all()

    full_h3_list = h3_ids
    if h3_ids is None:
        full_h3_list = get_all_h3_hexagons(res)
    
    full_h3_geo = [fix_h3_geometry(i) for i in full_h3_list]
    h3_geo = gpd.GeoSeries(full_h3_geo, index=full_h3_list, crs=4326)
    
    h3_intersects = h3_geo.sindex.query(spatial, predicate='intersects')
    return h3_geo.index[h3_intersects].unique().tolist()

def h3_index_df(df, res=12, part=3, lat_col='lat_lowestmode', lon_col='lon_lowestmode'):
    import h3pandas
    df = df.dropna(subset=[lat_col, lon_col])
    df = df.reset_index()
    df = df.h3.geo_to_h3(res, lat_col=lat_col, lng_col=lon_col, set_index=True)
    df = df.h3.h3_to_parent(part)
    return df.copy()


def h3_parts_to_gdf(h3_ids, crs=4326):
    """
    Convert H3 partition IDs to a GeoDataFrame with polygon geometries.

    Parameters
    ----------
    h3_ids : list
        List of H3 cell IDs (strings)
    crs : int or str
        Output CRS (default: 4326)

    Returns
    -------
    GeoDataFrame
        GeoDataFrame indexed by H3 ID with polygon geometries
    """
    import geopandas as gpd
    geometries = [fix_h3_geometry(h) for h in h3_ids]
    gdf = gpd.GeoDataFrame(
        {'h3_id': h3_ids},
        geometry=geometries,
        crs=4326
    ).set_index('h3_id')

    if crs != 4326:
        gdf = gdf.to_crs(crs)

    return gdf