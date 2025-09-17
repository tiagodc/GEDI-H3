import h3
from shapely.geometry import Polygon
import antimeridian

def get_all_h3_hexagons(resolution):
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

def iter_all_h3_hexagons(resolution):
    """Memory-efficient iterator for all H3 hexagons at resolution"""
    base_cells = h3.get_res0_cells()

    for base_cell in base_cells:
        children = h3.cell_to_children(base_cell, resolution)
        for child in children:
            yield child

def fix_h3_geometry(hex='805bfffffffffff'):
    """Using the antimeridian package for robust handling."""
    boundary_coords = h3.cell_to_boundary(hex)    
    polygon = Polygon([(lon, lat) for lat, lon in boundary_coords])
    
    # Fix antimeridian crossing
    fixed_geometry = antimeridian.fix_polygon(polygon)
    return fixed_geometry