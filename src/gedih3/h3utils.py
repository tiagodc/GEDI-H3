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

def intersect_h3_geometries(spatial, res=3, h3_ids=None, expand_ring=1):
    """Return H3 cells whose data footprint may intersect ``spatial``.

    H3 children are not geometrically contained in their parents: a shot
    stored under partition ``cell_to_parent(latlng_to_cell(lon, lat, index_res),
    part_res)`` can sit up to ~0.14-0.16 x the parent's edge length outside
    the parent's own polygon (~8-10 km at partition level 3; see
    ``_H3_OVERHANG_FRACTION`` in utils.py). A ROI touching that overhang
    band intersects the polygon the shots physically fall in, not the
    partition they are stored in — so an exact polygon intersection
    silently misses boundary shots (observed in production on the EGI
    path: ~5% of one L3 partition's shots; see ``egi_h3_intersection``).

    ``expand_ring=1`` (default) closes this by adding the grid_disk ring-1
    neighbors of every polygon-intersecting cell, restricted to the
    candidate set (``h3_ids`` when given). This is guaranteed sufficient:
    the overhang (~0.18 x edge) is far smaller than one cell width, so the
    true storage partition of any shot inside a polygon is always that
    polygon's cell or an immediate neighbor. Callers that need the exact
    polygon intersection can pass ``expand_ring=0``.

    Parameters
    ----------
    spatial : str | list | GeoSeries | GeoDataFrame | shapely geometry
        ROI as a vector-file path / "W,S,E,N" string, [W,S,E,N] bbox list,
        geopandas object, or shapely geometry (EPSG:4326).
    res : int
        H3 resolution of the candidate cells (ignored when ``h3_ids`` given).
    h3_ids : list, optional
        Candidate cell set (e.g. a database's partition ids). When given,
        both the intersection and the ring expansion are restricted to it.
    expand_ring : int, default 1
        ``grid_disk`` radius for the overhang-safety expansion; 0 disables.
    """
    from shapely.geometry import box
    from shapely.geometry.base import BaseGeometry
    import geopandas as gpd
    if isinstance(spatial, str):
        # Mirror the CLI's parse_region semantics so the Python API can
        # accept "region.shp" / "region.gpkg" / "region.geojson" / "W,S,E,N"
        # the same way `gh3_extract -r` does. The gh3_load docstring example
        # advertises this. Without it, sindex.query() raises a misleading
        # "Array should be of object dtype" downstream.
        from .cliutils import parse_region
        spatial = parse_region(spatial)
    if isinstance(spatial, list):
        spatial = box(*spatial)
    elif isinstance(spatial, gpd.GeoSeries) or isinstance(spatial, gpd.GeoDataFrame):
        spatial = spatial.to_crs(4326).union_all()
    elif not isinstance(spatial, BaseGeometry):
        raise TypeError(
            f"Unsupported region type {type(spatial).__name__}; "
            "expected a path/bbox-string, a list [W,S,E,N], a GeoDataFrame/GeoSeries, or a shapely geometry."
        )

    full_h3_list = h3_ids
    if h3_ids is None:
        full_h3_list = get_all_h3_hexagons(res)
    
    full_h3_geo = [fix_h3_geometry(i) for i in full_h3_list]
    h3_geo = gpd.GeoSeries(full_h3_geo, index=full_h3_list, crs=4326)
    
    h3_intersects = h3_geo.sindex.query(spatial, predicate='intersects')
    hits = h3_geo.index[h3_intersects].unique().tolist()

    if expand_ring and hits:
        # Mirror egi_h3_intersection: expand by grid_disk neighbors filtered
        # to the candidate set, so boundary shots stored in a partition whose
        # polygon doesn't touch the ROI are still selected.
        valid = set(full_h3_list)
        expanded = set(hits)
        for cell in hits:
            for nbr in h3.grid_disk(cell, expand_ring):
                if nbr in valid:
                    expanded.add(nbr)
        hits = sorted(expanded)

    return hits

def h3_index_df(df, res=12, part=3, lat_col='lat_lowestmode', lon_col='lon_lowestmode'):
    import warnings
    import pandas as pd
    import h3pandas
    # h3pandas's geo_to_h3 / h3_to_parent call frame.assign() internally,
    # which on wide GEDI frames (~30+ columns) triggers pandas's fragmentation
    # heuristic and emits a PerformanceWarning on every per-task call. The
    # final df.copy() below defragments the result so the warning is purely
    # cosmetic; suppress it to keep worker logs clean.
    with warnings.catch_warnings():
        warnings.filterwarnings(
            'ignore',
            message='DataFrame is highly fragmented',
            category=pd.errors.PerformanceWarning,
        )
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