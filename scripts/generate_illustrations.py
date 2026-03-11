#!/usr/bin/env python3
"""
generate_illustrations.py — Generate gedih3 documentation figures.

Figures produced:
  1. gedi_tracks.png          — GEDI orbital track pattern (real data)
  2. lidar_waveform.png       — Full-waveform LiDAR schematic (synthetic)
  3. h3_multi_resolution.png  — H3 hexagons at levels 3, 6, 9 (real data)
  4. h3_two_level.png         — H3 dual-level partition+index structure
  5. h3_boundary.png          — H3 parent/child boundary nesting caveat
  6. h3_egi_nesting.png       — H3 vs EGI hierarchical nesting comparison (conceptual)
  7. h3_vs_egi.png            — H3 level-7 hexagons vs EGI level-7 square pixels (real data)
  8. regression_r2.png        — Per-hexagon AGBD~height R² map (real data)

Usage:
  conda run -n gh3_dev python scripts/generate_illustrations.py
  conda run -n gh3_dev python scripts/generate_illustrations.py \\
      --db E:/gedih3/h3_database --out docs/imgs
"""
import argparse
import warnings
from pathlib import Path

import geopandas as gpd
import h3
import matplotlib.patches as mpatches
import matplotlib.pyplot as plt
import numpy as np
import pandas as pd
import shapely.geometry as shg
from matplotlib.patches import FancyArrowPatch
from shapely.geometry import box, mapping

warnings.filterwarnings("ignore")

# ─── Defaults ────────────────────────────────────────────────────────────────
DEFAULT_DB = "E:/gedih3/h3_database"
DEFAULT_OUT = "docs/imgs"

# Study region
BBOX = (-51.0, 0.0, -50.0, 1.0)  # W, S, E, N

# ─── Style ───────────────────────────────────────────────────────────────────
HEX_COLOR = "#2563EB"
EGI_COLOR = "#DC2626"
SHOT_COLOR = "#1a1a1a"
CMAP_AGBD = "YlGn"
CMAP_R2 = "viridis"

RCPARAMS = {
    "font.family": "sans-serif",
    "font.size": 8,
    "axes.linewidth": 0.5,
    "xtick.major.width": 0.5,
    "ytick.major.width": 0.5,
    "xtick.minor.width": 0.3,
    "ytick.minor.width": 0.3,
}


def setup():
    plt.rcParams.update(RCPARAMS)


def save_fig(fig, name, out):
    path = Path(out) / name
    fig.savefig(path, dpi=150, bbox_inches="tight", facecolor="white")
    plt.close(fig)
    print(f"  saved {path.name}")


def h3_cell_poly(cell):
    """H3 cell → shapely Polygon (lon, lat order)."""
    return shg.Polygon(
        [(lng, lat) for lat, lng in h3.cell_to_boundary(cell)]
    )


def cells_to_gdf(cells, crs=4326):
    polys = [h3_cell_poly(c) for c in cells]
    return gpd.GeoDataFrame({"cell": list(cells)}, geometry=polys, crs=crs)


def hide_spines(ax, which=("top", "right")):
    for s in which:
        ax.spines[s].set_visible(False)


# ─── Data loading ─────────────────────────────────────────────────────────────
def load_shots(db_path):
    """Load all shots with key columns. Returns a pandas GeoDataFrame."""
    import gedih3.gh3driver as gh3

    ddf = gh3.gh3_load(
        source=db_path,
        columns=[
            "lat_lowestmode_l2a",
            "lon_lowestmode_l2a",
            "agbd_l4a",
            "rh_098_l2a",
            "l4_quality_flag_l4a",
            "quality_flag_l2a",
            "datetime",
        ],
    )
    return ddf.compute()


def load_aggregated(db_path, gdf_shots, region=None):
    """Compute H3 level-7 and EGI level-7 aggregated AGBD (quality-filtered).

    H3 aggregation uses Dask (partition-level, no shuffle).
    EGI aggregation uses the lower-level pandas egi_dataframe/egi_aggregate
    directly on the already-loaded shots to avoid Dask metadata issues.

    Parameters
    ----------
    region : GeoDataFrame or bbox tuple (W, S, E, N), optional
        Spatial filter for constraining the output extent.
    """
    import gedih3.gh3driver as gh3
    from gedih3.egi.dataframe import egi_dataframe, egi_aggregate as egi_agg_fn

    # ── H3 aggregation (Dask, efficient) ─────────────────────────────────────
    load_cols = ["agbd_l4a"]
    if "l4_quality_flag_l4a" in gdf_shots.columns:
        load_cols.append("l4_quality_flag_l4a")

    ddf = gh3.gh3_load(source=db_path, columns=load_cols, region=region)
    if "l4_quality_flag_l4a" in ddf.columns:
        ddf_q = ddf.query("l4_quality_flag_l4a == 1 and agbd_l4a > 0")
    else:
        ddf_q = ddf.query("agbd_l4a > 0")
    h3_agg = gh3.gh3_aggregate(ddf_q, target_res=7, agg="mean").compute()

    # ── EGI aggregation (pandas, using already-loaded shots) ─────────────────
    conditions = (gdf_shots["agbd_l4a"] > 0) & gdf_shots["agbd_l4a"].notna()
    if "l4_quality_flag_l4a" in gdf_shots.columns:
        conditions = conditions & (gdf_shots["l4_quality_flag_l4a"] == 1)
    shots_q = gdf_shots.loc[conditions, ["agbd_l4a", "lon_lowestmode_l2a", "lat_lowestmode_l2a"]].copy()

    # Clip to region bbox if provided
    if region is not None:
        if hasattr(region, "total_bounds"):
            w, s, e, n = region.total_bounds
        else:
            w, s, e, n = region
        shots_q = shots_q[
            (shots_q["lon_lowestmode_l2a"] >= w)
            & (shots_q["lon_lowestmode_l2a"] <= e)
            & (shots_q["lat_lowestmode_l2a"] >= s)
            & (shots_q["lat_lowestmode_l2a"] <= n)
        ]

    shots_egi = egi_dataframe(
        shots_q,
        x_col="lon_lowestmode_l2a",
        y_col="lat_lowestmode_l2a",
        level=7,
    )
    # Keep only numeric columns for aggregation
    num_cols = shots_egi.select_dtypes(include="number").columns.tolist()
    egi_agg = egi_agg_fn(shots_egi[num_cols], mapper="mean", return_geometry=True)

    return h3_agg, egi_agg


# ─── Figure 1: GEDI orbital track pattern ────────────────────────────────────
def fig_gedi_tracks(gdf, out):
    """Scatter GEDI shot locations colored by year, showing orbit tracks."""
    setup()
    fig, ax = plt.subplots(figsize=(4.5, 4.5))

    gdf = gdf.dropna(subset=["lon_lowestmode_l2a", "lat_lowestmode_l2a"])
    gdf = gdf.copy()
    gdf["year"] = pd.to_datetime(gdf["datetime"]).dt.year

    years = sorted(gdf["year"].unique())
    cmap = plt.get_cmap("tab10", len(years))
    year_colors = {y: cmap(i) for i, y in enumerate(years)}

    for yr, grp in gdf.groupby("year"):
        ax.scatter(
            grp["lon_lowestmode_l2a"],
            grp["lat_lowestmode_l2a"],
            s=0.05,
            color=year_colors[yr],
            alpha=0.6,
            label=str(yr),
            rasterized=True,
        )

    # Study area boundary
    rx, ry = box(*BBOX).exterior.xy
    ax.plot(rx, ry, color="#888888", lw=0.8, ls="--", zorder=5)

    ax.set_xlim(BBOX[0] - 0.02, BBOX[2] + 0.02)
    ax.set_ylim(BBOX[1] - 0.02, BBOX[3] + 0.02)
    ax.set_aspect("equal")
    ax.set_xlabel("Longitude (°)")
    ax.set_ylabel("Latitude (°)")
    ax.set_title("GEDI orbital tracks — shot locations by year")
    hide_spines(ax)

    handles = [
        mpatches.Patch(color=year_colors[y], label=str(y)) for y in years
    ]
    ax.legend(
        handles=handles,
        title="Year",
        fontsize=6,
        title_fontsize=7,
        markerscale=4,
        frameon=False,
        loc="lower right",
    )

    save_fig(fig, "gedi_tracks.png", out)


# ─── Figure 2: LiDAR full-waveform schematic ─────────────────────────────────
def fig_lidar_waveform(out):
    """Synthetic full-waveform LiDAR diagram with RH annotations."""
    setup()
    fig, ax = plt.subplots(figsize=(3.8, 5.0))

    z = np.linspace(0, 40, 800)
    ground = np.exp(-((z - 1.5) ** 2) / (2 * 0.6 ** 2))
    canopy_low = 0.3 * np.exp(-((z - 14) ** 2) / (2 * 3.0 ** 2))
    canopy_high = 0.65 * np.exp(-((z - 27) ** 2) / (2 * 4.5 ** 2))
    waveform = ground + canopy_low + canopy_high
    waveform /= waveform.max()

    ax.plot(waveform, z, color="#1a6fad", lw=1.6)
    ax.fill_betweenx(z, 0, waveform, alpha=0.15, color="#1a6fad")

    # RH annotations — fixed illustrative heights
    rh_heights = {25: 10, 50: 18, 75: 25, 98: 35, 100: 39}
    rh_colors = {
        25: "#9b59b6",
        50: "#3498db",
        75: "#27ae60",
        98: "#e67e22",
        100: "#e74c3c",
    }
    for pct, h in rh_heights.items():
        ax.axhline(h, color=rh_colors[pct], lw=0.8, ls="--", alpha=0.85)
        ax.text(
            1.02,
            h,
            f"RH{pct:d}",
            transform=ax.get_yaxis_transform(),
            va="center",
            fontsize=6.5,
            color=rh_colors[pct],
        )

    # Label ground and canopy return peaks
    ax.annotate(
        "Ground\nreturn",
        xy=(0.78, 1.5),
        xytext=(0.55, 6),
        arrowprops=dict(arrowstyle="-|>", color="#555", lw=0.7),
        fontsize=7,
        color="#333",
        ha="center",
    )
    ax.annotate(
        "Canopy\nreturn",
        xy=(0.65, 27),
        xytext=(0.45, 34),
        arrowprops=dict(arrowstyle="-|>", color="#555", lw=0.7),
        fontsize=7,
        color="#333",
        ha="center",
    )

    ax.set_xlabel("Waveform energy (normalised)")
    ax.set_ylabel("Height above ground (m)")
    ax.set_xlim(-0.04, 1.15)
    ax.set_ylim(-2, 42)
    ax.set_title("Full-waveform LiDAR return")
    hide_spines(ax)

    save_fig(fig, "lidar_waveform.png", out)



# ─── Figure 4: H3 multi-resolution ───────────────────────────────────────────
def fig_h3_multi_resolution(gdf, out):
    """Three-panel: H3 hexagons at levels 3, 6, 9 over the study area."""
    setup()
    fig, axes = plt.subplots(1, 3, figsize=(10, 3.8))

    _bbox = [-52.0, -1.0, -49.0, 2.0]  # slightly larger bbox for better hex coverage
    region_geojson = mapping(box(*_bbox))
    area_labels = {3: "~12,400 km²", 5: "~252 km²", 7: "~5 km²"}

    shots_sub = gdf.sample(min(5000, len(gdf)), random_state=42) if len(gdf) > 5000 else gdf

    for ax, res in zip(axes, [3, 5, 7]):
        cells = list(h3.geo_to_cells(region_geojson, res))
        hex_gdf = cells_to_gdf(cells)

        hex_gdf.plot(
            ax=ax,
            facecolor="none",
            edgecolor=HEX_COLOR,
            linewidth=0.7 if res <= 6 else 0.3,
        )
        ax.scatter(
            shots_sub["lon_lowestmode_l2a"],
            shots_sub["lat_lowestmode_l2a"],
            s=0.04 if res <= 6 else 0.02,
            color=SHOT_COLOR,
            alpha=0.35,
            rasterized=True,
        )

        ax.set_xlim(_bbox[0], _bbox[2])
        ax.set_ylim(_bbox[1], _bbox[3])        
        
        ax.set_aspect("equal")
        ax.set_title(f"H3 level {res}\n(cell area {area_labels[res]})", fontsize=8)

        if ax is axes[0]:
            ax.set_xlabel("Longitude (°)")
            ax.set_ylabel("Latitude (°)")
        else:
            ax.set_xticklabels([])
            ax.set_yticklabels([])

        hide_spines(ax)

    plt.suptitle("H3 hexagonal indexing at multiple resolutions", y=1.01, fontsize=9)
    plt.tight_layout()
    save_fig(fig, "h3_multi_resolution.png", out)


# ─── Figure 5: H3 dual-level structure ───────────────────────────────────────
def fig_h3_two_level(db_path, out):
    """Two-panel: level-3 partition with shots (left) + zoomed level-12 cells (right)."""
    import gedih3.gh3driver as gh3
    from matplotlib.patches import ConnectionPatch, Rectangle

    setup()
    ZOOM_COLOR = "#E65100"  # orange for the zoom rectangle + right border

    # Load real data from a specific partition
    part_id = "83804cfffffffff"
    part_dir = str(Path(db_path) / f"h3_03={part_id}")
    df_full = gh3.gh3_load_hex(part_dir, columns=["lat_lowestmode_l2a", "lon_lowestmode_l2a"])

    lons_full = df_full["lon_lowestmode_l2a"].values
    lats_full = df_full["lat_lowestmode_l2a"].values
    h3_12_full = df_full.index if df_full.index.name == "h3_12" else df_full["h3_12"]

    part_poly = h3_cell_poly(part_id)
    part_gdf = gpd.GeoDataFrame(geometry=[part_poly], crs=4326)

    # ── Find densest ~500 m area via fine binning on full data ───────────────
    bin_size = 0.005
    lon_bins = ((lons_full - lons_full.min()) / bin_size).astype(int)
    lat_bins = ((lats_full - lats_full.min()) / bin_size).astype(int)
    bin_ids = lon_bins * 10000 + lat_bins
    counts = pd.Series(bin_ids).value_counts()
    best_bin = counts.index[0]
    best_lon_bin, best_lat_bin = divmod(best_bin, 10000)
    zoom_cx = lons_full.min() + (best_lon_bin + 0.5) * bin_size
    zoom_cy = lats_full.min() + (best_lat_bin + 0.5) * bin_size

    half_w = 0.003  # ~0.006° ≈ 670 m window
    zoom_x0, zoom_y0 = zoom_cx - half_w, zoom_cy - half_w
    zoom_x1, zoom_y1 = zoom_cx + half_w, zoom_cy + half_w

    # Zoom mask on full data
    mask_zoom = (
        (lons_full >= zoom_x0) & (lons_full <= zoom_x1) &
        (lats_full >= zoom_y0) & (lats_full <= zoom_y1)
    )
    zoom_lons = lons_full[mask_zoom]
    zoom_lats = lats_full[mask_zoom]
    zoom_cells = set(h3_12_full[mask_zoom])

    # Sample for left panel plotting speed
    if len(df_full) > 15000:
        df_sampled = df_full.sample(15000, random_state=42)
    else:
        df_sampled = df_full
    lons = df_sampled["lon_lowestmode_l2a"].values
    lats = df_sampled["lat_lowestmode_l2a"].values

    # ── Two-panel figure ─────────────────────────────────────────────────────
    fig, (ax_l, ax_r) = plt.subplots(1, 2, figsize=(8, 4))

    # ── Left panel: full partition ───────────────────────────────────────────
    part_gdf.boundary.plot(ax=ax_l, color=HEX_COLOR, linewidth=1.8, zorder=5)
    ax_l.scatter(
        lons, lats,
        s=0.3, color=SHOT_COLOR, alpha=0.5, zorder=6, rasterized=True,
    )

    # Orange zoom rectangle on left panel
    rect = Rectangle(
        (zoom_x0, zoom_y0), zoom_x1 - zoom_x0, zoom_y1 - zoom_y0,
        linewidth=1.5, edgecolor=ZOOM_COLOR, facecolor="none", zorder=10,
    )
    ax_l.add_patch(rect)

    cx = part_poly.centroid.x
    ax_l.set_title("H3 level-3 partition (~12,400 km²)",
                    fontsize=8, color=HEX_COLOR, fontweight="bold")
    ax_l.set_aspect("equal")
    ax_l.set_xlabel("Longitude (°)")
    ax_l.set_ylabel("Latitude (°)")
    hide_spines(ax_l)

    # ── Right panel: zoomed H3 level-12 cells ────────────────────────────────
    if zoom_cells:
        hex_gdf = cells_to_gdf(list(zoom_cells))
        hex_gdf.plot(
            ax=ax_r,
            facecolor="#dbeafe",
            edgecolor="#93c5fd",
            linewidth=0.8,
            alpha=0.8,
        )

    ax_r.scatter(
        zoom_lons, zoom_lats,
        s=8, color=SHOT_COLOR, alpha=0.8, zorder=6, rasterized=True,
    )

    ax_r.set_xlim(zoom_x0, zoom_x1)
    ax_r.set_ylim(zoom_y0, zoom_y1)
    ax_r.set_aspect("equal")
    ax_r.set_title("H3 level-12 index cells (~307 m²)",
                    fontsize=8, color=ZOOM_COLOR, fontweight="bold")
    ax_r.set_xlabel("Longitude (°)")
    hide_spines(ax_r)

    # Orange border on right panel to match the zoom rectangle
    for spine in ax_r.spines.values():
        spine.set_edgecolor(ZOOM_COLOR)
        spine.set_linewidth(1.5)
        spine.set_visible(True)

    # ── Connector lines between panels ───────────────────────────────────────
    for (x_l, y_l), (x_r, y_r) in [
        ((zoom_x1, zoom_y1), (zoom_x0, zoom_y1)),  # top-right → top-left
        ((zoom_x1, zoom_y0), (zoom_x0, zoom_y0)),  # bot-right → bot-left
    ]:
        con = ConnectionPatch(
            xyA=(x_l, y_l), coordsA=ax_l.transData,
            xyB=(x_r, y_r), coordsB=ax_r.transData,
            color=ZOOM_COLOR, linewidth=0.8, linestyle="--", alpha=0.7,
        )
        fig.add_artist(con)

    fig.suptitle("H3 dual-level structure", fontsize=10, y=1.02)
    plt.tight_layout()
    save_fig(fig, "h3_two_level.png", out)


# ─── Figure 6: H3 boundary / parent-child nesting ────────────────────────────
def fig_h3_boundary(out):
    """Two adjacent level-3 hexes with hierarchical children centroids (Gosper Island)."""
    setup()
    fig, ax = plt.subplots(figsize=(4.5, 4.5))

    # Two adjacent level-3 hexagons
    center_lat = (BBOX[1] + BBOX[3]) / 2
    center_lon = (BBOX[0] + BBOX[2]) / 2
    seed_cell = h3.latlng_to_cell(center_lat, center_lon, 3)
    neighbors = list(h3.grid_disk(seed_cell, 1))
    neighbor = [n for n in neighbors if n != seed_cell][0]

    colors = {"A": HEX_COLOR, "B": "#d97706"}
    fill_colors = {"A": "#dbeafe", "B": "#fef3c7"}
    label_map = {seed_cell: "A", neighbor: "B"}

    # Hierarchical children at level 7 (~2,401 per parent) — plotted as hexagons
    child_level = 7
    for p_id in [seed_cell, neighbor]:
        lbl = label_map[p_id]
        children = list(h3.cell_to_children(p_id, child_level))
        child_gdf = cells_to_gdf(children)
        child_gdf.plot(
            ax=ax,
            facecolor=fill_colors[lbl],
            edgecolor=colors[lbl],
            linewidth=0.15,
            alpha=0.9,
            zorder=4,
        )

    # Parent hexagon outlines on top
    for p_id in [seed_cell, neighbor]:
        poly = h3_cell_poly(p_id)
        lbl = label_map[p_id]
        ax.plot(*poly.exterior.xy, color=colors[lbl], lw=1.5, zorder=5)
        cx, cy = poly.centroid.x, poly.centroid.y
        ax.text(cx, cy, f"Cell {lbl}", ha="center", va="center",
                fontsize=8, color=colors[lbl], fontweight="bold", zorder=6)

    ax.set_aspect("equal")
    ax.set_xlabel("Longitude (°)")
    ax.set_ylabel("Latitude (°)")
    ax.set_title("H3 parent/child nesting pattern", fontsize=9)
    legend_handles = [
        mpatches.Patch(facecolor=fill_colors["A"], edgecolor=colors["A"], label="Children of cell A"),
        mpatches.Patch(facecolor=fill_colors["B"], edgecolor=colors["B"], label="Children of cell B"),
    ]
    ax.legend(handles=legend_handles, fontsize=7, frameon=False, loc="upper right")
    hide_spines(ax)

    save_fig(fig, "h3_boundary.png", out)


# ─── Figure 7: H3 hexagons vs EGI square pixels ──────────────────────────────
def fig_h3_vs_egi(h3_agg, egi_agg, out):
    """Side-by-side: H3 level-7 AGBD hexagons and EGI level-7 AGBD squares."""
    setup()
    fig, (ax1, ax2) = plt.subplots(1, 2, figsize=(9, 4))

    col_h3 = "agbd_l4a"
    col_egi = "agbd_l4a"

    h3_vals = h3_agg[col_h3].dropna()
    egi_vals = egi_agg[col_egi].dropna()
    vmin = min(h3_vals.quantile(0.05), egi_vals.quantile(0.05))
    vmax = max(h3_vals.quantile(0.95), egi_vals.quantile(0.95))

    sm = plt.cm.ScalarMappable(
        cmap=CMAP_AGBD,
        norm=plt.Normalize(vmin=vmin, vmax=vmax),
    )
    sm.set_array([])

    # H3 panel
    h3_agg.plot(
        column=col_h3,
        ax=ax1,
        cmap=CMAP_AGBD,
        vmin=vmin,
        vmax=vmax,
        edgecolor="white",
        linewidth=0.15,
        legend=False,
        missing_kwds={"color": "#f3f4f6"},
    )
    ax1.set_title("H3 hexagons  (level 7, ~5 km²)", fontsize=8)
    ax1.set_xlabel("Longitude (°)")
    ax1.set_ylabel("Latitude (°)")
    hide_spines(ax1)

    # EGI panel
    egi_plot = egi_agg.to_crs(4326) if egi_agg.crs and egi_agg.crs.to_epsg() != 4326 else egi_agg
    egi_plot.plot(
        column=col_egi,
        ax=ax2,
        cmap=CMAP_AGBD,
        vmin=vmin,
        vmax=vmax,
        edgecolor="white",
        linewidth=0.05,
        legend=False,
        missing_kwds={"color": "#f3f4f6"},
    )
    ax2.set_title("EGI square pixels  (level 7, ~2 km)", fontsize=8)
    ax2.set_xlabel("Longitude (°)")
    ax2.set_yticklabels([])
    hide_spines(ax2)

    # Sync axes extents from data bounds (with small padding)
    all_bounds = h3_agg.total_bounds  # [minx, miny, maxx, maxy]
    egi_bounds = egi_plot.total_bounds
    xmin = min(all_bounds[0], egi_bounds[0]) - 0.02
    ymin = min(all_bounds[1], egi_bounds[1]) - 0.02
    xmax = max(all_bounds[2], egi_bounds[2]) + 0.02
    ymax = max(all_bounds[3], egi_bounds[3]) + 0.02
    for ax in (ax1, ax2):
        ax.set_xlim(xmin, xmax)
        ax.set_ylim(ymin, ymax)

    fig.subplots_adjust(right=0.88, wspace=0.08)
    cax = fig.add_axes([0.90, 0.15, 0.02, 0.7])
    cbar = fig.colorbar(sm, cax=cax)
    cbar.set_label("Mean AGBD (Mg ha⁻¹)", fontsize=7)
    cbar.ax.tick_params(labelsize=6)

    fig.suptitle("H3 hexagons vs EGI square pixels — same AGBD data", fontsize=9)
    save_fig(fig, "h3_vs_egi.png", out)


# ─── Figure 8: H3 vs EGI hierarchical nesting ────────────────────────────────
def fig_h3_egi_nesting(out):
    """Side-by-side: H3 and EGI hierarchical nesting (conceptual, no data)."""
    import gedih3.egi.core as egi_core
    import gedih3.egi.spatial as egi_spatial

    setup()
    fig, (ax_h3, ax_egi) = plt.subplots(1, 2, figsize=(10, 4))

    colors = {"coarse": "#1a1a1a", "mid": "#2563eb", "fine": "#e07b39"}

    # ── H3 panel (left) ──────────────────────────────────────────────────────
    seed = h3.latlng_to_cell(0.5, -50.5, 3)
    children5 = list(h3.cell_to_children(seed, 5))
    children6 = list(h3.cell_to_children(seed, 6))

    # Finest first (bottom), coarsest on top
    cells_to_gdf(children6).plot(
        ax=ax_h3, facecolor="none", edgecolor=colors["fine"],
        linewidth=0.3, zorder=2,
    )
    cells_to_gdf(children5).plot(
        ax=ax_h3, facecolor="none", edgecolor=colors["mid"],
        linewidth=0.6, zorder=3,
    )
    parent_poly = h3_cell_poly(seed)
    ax_h3.plot(*parent_poly.exterior.xy, color=colors["coarse"], lw=2.0, zorder=4)

    ax_h3.set_title("H3 hexagons  (levels 3 → 5 → 6)", fontsize=8)
    ax_h3.set_xlabel("Longitude (°)")
    ax_h3.set_ylabel("Latitude (°)")
    ax_h3.set_aspect("equal")
    hide_spines(ax_h3)

    # ── EGI panel (right) ────────────────────────────────────────────────────
    # Find EGI level-12 tile near the H3 cell
    h3_cx, h3_cy = parent_poly.centroid.x, parent_poly.centroid.y
    pt_6933 = gpd.GeoDataFrame(
        geometry=gpd.points_from_xy([h3_cx], [h3_cy]), crs=4326,
    ).to_crs(6933)
    cx6933, cy6933 = pt_6933.geometry[0].x, pt_6933.geometry[0].y
    hash12 = egi_core.to_hash(cx6933, cy6933, level=12)

    children9 = egi_core.get_children(np.uint64(hash12), children_level=9)
    children8 = egi_core.get_children(np.uint64(hash12), children_level=8)

    # Convert to GeoDataFrames in WGS84
    gdf8 = egi_spatial.to_geodataframe(children8).to_crs(4326)
    gdf9 = egi_spatial.to_geodataframe(children9).to_crs(4326)
    parent12 = gpd.GeoDataFrame(
        geometry=[egi_spatial.pixel_shape(np.uint64(hash12))], crs=6933,
    ).to_crs(4326)

    gdf8.plot(
        ax=ax_egi, facecolor="none", edgecolor=colors["fine"],
        linewidth=0.3, zorder=2,
    )
    gdf9.plot(
        ax=ax_egi, facecolor="none", edgecolor=colors["mid"],
        linewidth=0.6, zorder=3,
    )
    ax_egi.plot(
        *parent12.geometry[0].exterior.xy,
        color=colors["coarse"], lw=2.0, zorder=4,
    )

    ax_egi.set_title("EGI square pixels  (levels 12 → 9 → 8)", fontsize=8)
    ax_egi.set_xlabel("Longitude (°)")
    ax_egi.set_yticklabels([])
    ax_egi.set_aspect("equal")
    hide_spines(ax_egi)

    # ── Sync extents ─────────────────────────────────────────────────────────
    for ax in (ax_h3, ax_egi):
        ax.set_xlim(-51, -48.5)
        ax.set_ylim(-1, 1)

    # ── Legend ────────────────────────────────────────────────────────────────
    legend_handles = [
        mpatches.Patch(facecolor="none", edgecolor=colors["coarse"], lw=2.0,
                       label="Coarsest  (H3 L3 / EGI L12)"),
        mpatches.Patch(facecolor="none", edgecolor=colors["mid"], lw=1.0,
                       label="Mid-level  (H3 L5 / EGI L9)"),
        mpatches.Patch(facecolor="none", edgecolor=colors["fine"], lw=0.5,
                       label="Finest  (H3 L6 / EGI L8)"),
    ]
    fig.legend(handles=legend_handles, loc="lower center", ncol=3,
               fontsize=7, frameon=False, bbox_to_anchor=(0.5, -0.02))

    fig.suptitle("Hierarchical nesting — H3 vs EGI", fontsize=9)
    fig.subplots_adjust(wspace=0.08, bottom=0.12)
    save_fig(fig, "h3_egi_nesting.png", out)


# ─── Figure 9: Per-hexagon regression R² and slope significance ──────────────
def fig_regression_r2(gdf_shots, out):
    """Two-panel: R² (left) and slope p-value (right) for AGBD ~ RH98 at H3 level 7.

    Uses scipy.stats.linregress for both R² and slope p-value.
    """
    from matplotlib.colors import LogNorm
    from scipy.stats import linregress

    setup()

    # Quality filter + spatial clip to study region
    mask = (
        (gdf_shots["l4_quality_flag_l4a"] == 1)
        & (gdf_shots["quality_flag_l2a"] == 1)
        & gdf_shots["agbd_l4a"].notna()
        & gdf_shots["rh_098_l2a"].notna()
        & (gdf_shots["agbd_l4a"] > 0)
        & (gdf_shots["lon_lowestmode_l2a"] >= BBOX[0])
        & (gdf_shots["lon_lowestmode_l2a"] <= BBOX[2])
        & (gdf_shots["lat_lowestmode_l2a"] >= BBOX[1])
        & (gdf_shots["lat_lowestmode_l2a"] <= BBOX[3])
    )
    shots_q = gdf_shots[mask][["agbd_l4a", "rh_098_l2a"]].copy()

    # h3_12 is the GeoDataFrame index; reset to use as column
    if gdf_shots.index.name and gdf_shots.index.name.startswith("h3_"):
        shots_q["h3_12"] = gdf_shots[mask].index
    elif "h3_12" in gdf_shots.columns:
        shots_q["h3_12"] = gdf_shots[mask]["h3_12"]
    else:
        print("  [skip] regression_r2.png — no H3 index column found")
        return

    # Add H3 level-7 parent for each shot
    shots_q["h3_07"] = [h3.cell_to_parent(c, 7) for c in shots_q["h3_12"]]

    # Regression per hexagon (R², slope p-value)
    def fit_per_hex(df):
        n = len(df)
        if n < 5:
            return pd.Series({"r2": np.nan, "pvalue": np.nan, "n": n})
        res = linregress(df["rh_098_l2a"].values, df["agbd_l4a"].values)
        return pd.Series({"r2": float(res.rvalue ** 2), "pvalue": float(res.pvalue), "n": n})

    results = shots_q.groupby("h3_07").apply(fit_per_hex, include_groups=False).reset_index()
    results = results[(results["n"] >= 5) & (results["r2"] > 0)].dropna(subset=["r2", "pvalue"])

    if len(results) == 0:
        print("  [skip] regression_r2.png — insufficient data")
        return

    # Clamp p-values away from zero for log scale
    results["pvalue"] = results["pvalue"].clip(lower=1e-20)

    # Add polygon geometry
    results["geometry"] = [h3_cell_poly(c) for c in results["h3_07"]]
    results = gpd.GeoDataFrame(results, geometry="geometry", crs=4326)

    fig, (ax1, ax2) = plt.subplots(1, 2, figsize=(9, 4))

    # ── Left panel: R² ───────────────────────────────────────────────────────
    sm_r2 = plt.cm.ScalarMappable(cmap=CMAP_R2, norm=plt.Normalize(vmin=0, vmax=1))
    sm_r2.set_array([])
    results.plot(
        column="r2", ax=ax1, cmap=CMAP_R2, vmin=0, vmax=1,
        edgecolor="none", legend=False, missing_kwds={"color": "#f3f4f6"},
    )
    ax1.set_xlim(BBOX[0] - 0.02, BBOX[2] + 0.02)
    ax1.set_ylim(BBOX[1] - 0.02, BBOX[3] + 0.02)
    ax1.set_aspect("equal")
    ax1.set_xlabel("Longitude (°)")
    ax1.set_ylabel("Latitude (°)")
    ax1.set_title("R²  (AGBD ~ RH98)", fontsize=8)
    hide_spines(ax1)
    cbar1 = fig.colorbar(sm_r2, ax=ax1, shrink=0.85, pad=0.02)
    cbar1.set_label("R²", fontsize=7)
    cbar1.ax.tick_params(labelsize=6)

    # ── Right panel: slope p-value (log scale) ───────────────────────────────
    pmin = results["pvalue"].min()
    pmax = results["pvalue"].max()
    log_norm = LogNorm(vmin=max(pmin, 1e-20), vmax=min(pmax, 1.0))

    sm_pv = plt.cm.ScalarMappable(cmap="RdYlGn_r", norm=log_norm)
    sm_pv.set_array([])
    results.plot(
        column="pvalue", ax=ax2, cmap="RdYlGn_r", norm=log_norm,
        edgecolor="none", legend=False, missing_kwds={"color": "#f3f4f6"},
    )
    ax2.set_xlim(BBOX[0] - 0.02, BBOX[2] + 0.02)
    ax2.set_ylim(BBOX[1] - 0.02, BBOX[3] + 0.02)
    ax2.set_aspect("equal")
    ax2.set_xlabel("Longitude (°)")
    ax2.set_yticklabels([])
    ax2.set_title("Slope p-value  (95% significance)", fontsize=8)
    hide_spines(ax2)
    cbar2 = fig.colorbar(sm_pv, ax=ax2, shrink=0.85, pad=0.02)
    cbar2.set_label("p-value (slope)", fontsize=7)
    cbar2.ax.tick_params(labelsize=6)
    # Mark 0.05 threshold on colorbar
    cbar2.ax.axhline(0.05, color="black", lw=1.0, ls="--")
    cbar2.ax.text(1.3, 0.05, "p = 0.05", transform=cbar2.ax.get_yaxis_transform(),
                  va="center", fontsize=6, color="black")

    fig.suptitle("Per-hexagon AGBD ~ RH98 regression (H3 level 7)", fontsize=9)
    fig.subplots_adjust(wspace=0.08)
    save_fig(fig, "regression_r2.png", out)


# ─── Main ─────────────────────────────────────────────────────────────────────
def parse_args():
    p = argparse.ArgumentParser(description="Generate gedih3 documentation illustrations.")
    p.add_argument("--db", default=DEFAULT_DB, help="Path to H3 database")
    p.add_argument("--out", default=DEFAULT_OUT, help="Output directory for PNGs")
    p.add_argument("--only", type=int, default=None,
                   help="Generate only figure N (1-8), skip the rest")
    return p.parse_args()


def main():
    args = parse_args()
    out = Path(args.out)
    out.mkdir(parents=True, exist_ok=True)
    only = args.only

    print(f"Database : {args.db}")
    print(f"Output   : {out.resolve()}")
    if only is not None:
        print(f"Only     : figure {only}")
    print()

    # ── Conceptual figures (no data needed) ──────────────────────────────────
    if only is None or only == 1:
        print("[1/8] lidar_waveform.png  (conceptual)")
        fig_lidar_waveform(out)

    if only is None or only == 2:
        print("[2/8] h3_boundary.png  (H3 library only)")
        fig_h3_boundary(out)

    if only is None or only == 3:
        print("[3/8] h3_egi_nesting.png  (conceptual)")
        fig_h3_egi_nesting(out)

    # ── Load data once (needed for figures 4-8) ──────────────────────────────
    gdf = None
    if only is None or only >= 4:
        print("\nLoading shots from database…")
        gdf = load_shots(args.db)
        print(f"  {len(gdf):,} shots loaded")

    if only is None or only == 4:
        print("\n[4/8] gedi_tracks.png")
        fig_gedi_tracks(gdf, out)

    if only is None or only == 5:
        print("[5/8] h3_multi_resolution.png")
        fig_h3_multi_resolution(gdf, out)

    if only is None or only == 6:
        print("[6/8] h3_two_level.png")
        fig_h3_two_level(args.db, out)

    # ── Aggregated data (needed for figure 7) ────────────────────────────────
    if only is None or only == 7:
        print("\nAggregating data (H3 level 7 + EGI level 7)…")
        from gedih3.cliutils import parse_region
        h3_agg, egi_agg = load_aggregated(args.db, gdf, region=parse_region("-51,0,-50,1"))
        print(f"  H3 agg: {len(h3_agg):,} hexes  |  EGI agg: {len(egi_agg):,} pixels")

        print("\n[7/8] h3_vs_egi.png")
        fig_h3_vs_egi(h3_agg, egi_agg, out)

    # ── Regression ────────────────────────────────────────────────────────────
    if only is None or only == 8:
        print("[8/8] regression_r2.png")
        fig_regression_r2(gdf, out)

    print(f"\nDone — {len(list(out.glob('*.png')))} PNGs in {out.resolve()}")


if __name__ == "__main__":
    main()
