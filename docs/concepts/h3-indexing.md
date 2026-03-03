# H3 Hexagonal Indexing

## What is H3?

**H3** is a hierarchical geospatial indexing system developed by Uber. It divides the Earth's surface into a multi-resolution grid of hexagonal cells. Every point on Earth can be assigned to a unique H3 cell at any of 16 resolution levels (0–15), ranging from continental scale (~4.25 million km²) down to sub-meter scale (~0.90 m²).

H3 is the primary spatial index in gedih3 and serves as the backbone of the H3 database.

---

## Why Hexagons?

Hexagons have several geometric properties that make them well-suited for spatial indexing of point data like GEDI footprints:

- **Equal area (approximately)**: All hexagons at a given resolution level have nearly the same area, avoiding the distortion that affects square-pixel grids (where cells near poles are very different in area from cells near the equator).
- **Uniform adjacency**: Every hexagon has exactly 6 neighbors at equal distance, eliminating the diagonal-vs-cardinal asymmetry of square grids.
- **Compact shape**: Hexagons minimize the ratio of perimeter to area, reducing edge effects in spatial analyses.
- **Hierarchical nesting**: Each cell has a well-defined parent at the next coarser resolution level, enabling multi-resolution aggregation.

> **Suggested image**: A side-by-side comparison of GEDI shot locations indexed by H3 hexagons at resolution levels 3, 6, and 9, shown on a map. This would illustrate how the same data appears at different scales of aggregation.

---

## H3 Resolution Levels

| Level | Avg. Area | Edge Length | Typical Use |
|-------|-----------|-------------|-------------|
| 0 | ~4,250,547 km² | ~1,107 km | Continental |
| 1 | ~607,221 km² | ~418 km | Sub-continental |
| 2 | ~86,745 km² | ~158 km | Large country |
| 3 | ~12,393 km² | ~59 km | **Database partition (default)** |
| 4 | ~1,770 km² | ~22 km | Regional |
| 5 | ~253 km² | ~8.5 km | Landscape |
| 6 | ~36 km² | ~3.2 km | Regional aggregation |
| 7 | ~5.2 km² | ~1.2 km | Fine regional |
| 8 | ~0.74 km² | ~0.46 km | Local |
| 9 | ~0.105 km² | ~0.17 km | Local analysis |
| 10 | ~0.015 km² | ~65 m | Fine local |
| 11 | ~2,149 m² | ~25 m | GEDI footprint scale |
| 12 | ~307 m² | ~9.4 m | **Database index (default)** |
| 13 | ~44 m² | ~3.5 m | Very fine |
| 14 | ~6.3 m² | ~1.3 m | Ultra fine |
| 15 | ~0.90 m² | ~0.5 m | Maximum resolution |

---

## How gedih3 Uses H3: The Dual-Level System

gedih3 uses H3 at two different resolution levels simultaneously:

**Partition level** (default: level 3, ~12,000 km²)
The coarse H3 level used to organize files on disk. Each H3 partition cell becomes a directory in the database, containing all GEDI shots that fall within that geographic tile. This partitioning enables spatial queries to skip irrelevant tiles entirely — a query for "shots over Brazil" only reads the tiles that overlap Brazil.

**Index level** (default: level 12, ~307 m²)
The fine H3 level assigned to each individual GEDI shot. At level 12, each hexagon is roughly the size of a GEDI footprint, making this a natural resolution for shot-level indexing. This column is stored in every parquet file and is used for multi-resolution aggregation.

> **Suggested image**: A diagram showing the two-level H3 structure: a large level-3 hexagon (the partition tile) containing many small level-12 hexagons (the shot index), with GEDI shot dots distributed inside. This would clarify the partition vs. index concept intuitively.

```bash
# Customize index and partition levels at build time
gh3_build -r "-51,0,-50,1" -h3r 12 -h3p 3  # defaults
gh3_build -r "-51,0,-50,1" -h3r 11 -h3p 4  # finer index, larger partitions
```

---

## The Parent/Child Nesting Caveat

H3 is a hierarchical system, but hexagonal grids cannot be perfectly nested across resolution levels due to the geometry of tiling a sphere with hexagons. **H3 parent hexagons are not perfectly geometrically inclusive of their children.**

What this means in practice: when you compute the parent of a level-12 cell at level-6, the returned parent is the level-6 cell whose *center* is closest to the level-12 cell's center — not necessarily the cell that *geometrically contains* it. Near hexagon boundaries, a small fraction of child cells (typically 1–2%) may be assigned to a neighboring parent rather than the containing one.

> **Suggested image**: A close-up illustration of a hexagon boundary between two level-6 cells, with level-12 child cells colored by their computed parent. Some child cells near the boundary appear inside one parent's geometry but are assigned to the adjacent parent. This is the most important conceptual diagram for this section.

**How gedih3 handles this**: `gh3_aggregate` groups shots by their computed H3 parent cell (using `h3.cell_to_parent()`), which is consistent and fast. The assignment is deterministic and matches what H3 users expect — it is simply not a perfect geometric containment, which is a known property of H3 that users should be aware of when comparing cross-resolution results.

For the vast majority of analyses, this has negligible effect. It becomes relevant only if you are studying phenomena at or near H3 cell boundaries, or if you need exact geometric containment (in which case EGI square pixels may be more appropriate — see [EGI Indexing](egi-indexing.md)).

---

## H3 Aggregation in gedih3

```python
import gedih3.gh3driver as gh3

# Load shots from the H3 database
ddf = gh3.gh3_load(source='~/gedi_data/h3/', columns=['agbd_l4a'])

# Aggregate from level 12 (shot level) to level 6 (~36 km²)
# Each Dask partition is processed independently — no shuffle needed
agg = gh3.gh3_aggregate(ddf, target_res=6, agg='mean')
agg.compute()
```

See [Python API](../user-guide/python-api.md) for custom aggregation functions.

---

## Further Reading

- [H3 documentation](https://h3geo.org/docs/)
- [h3-py Python library](https://uber.github.io/h3-py/)
