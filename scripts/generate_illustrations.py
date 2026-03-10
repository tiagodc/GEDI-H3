#!/usr/bin/env python3
"""
generate_illustrations.py — Generate gedih3 documentation figures.

Figures produced:
  1. gedi_tracks.png          — GEDI orbital track pattern (real data)
  2. lidar_waveform.png       — Full-waveform LiDAR schematic (synthetic)
  3. hdf5_vs_h3.png           — HDF5 hierarchy → H3 parquet layout (conceptual)
  4. h3_multi_resolution.png  — H3 hexagons at levels 3, 6, 9 (real data)
  5. h3_two_level.png         — H3 dual-level partition+index structure
  6. h3_boundary.png          — H3 parent/child boundary nesting caveat
  7. h3_vs_egi.png            — H3 hexagons vs EGI square pixels (real data)
  8. egi_agbd_map.png         — EGI-aggregated AGBD map (real data)
  9. regression_r2.png        — Per-hexagon AGBD~height R² map (real data)

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


def load_aggregated(db_path, gdf_shots):
    """Compute H3 level-7 and EGI level-6 aggregated AGBD (quality-filtered).

    H3 aggregation uses Dask (partition-level, no shuffle).
    EGI aggregation uses the lower-level pandas egi_dataframe/egi_aggregate
    directly on the already-loaded shots to avoid Dask metadata issues.
    """
    import gedih3.gh3driver as gh3
    from gedih3.egi.dataframe import egi_dataframe, egi_aggregate as egi_agg_fn

    # ── H3 aggregation (Dask, efficient) ─────────────────────────────────────
    ddf = gh3.gh3_load(
        source=db_path,
        columns=["agbd_l4a", "l4_quality_flag_l4a"],
    )
    ddf_q = ddf.query("l4_quality_flag_l4a == 1 and agbd_l4a > 0")
    h3_agg = gh3.gh3_aggregate(ddf_q, target_res=7, agg="mean").compute()

    # ── EGI aggregation (pandas, using already-loaded shots) ─────────────────
    mask = (
        (gdf_shots["l4_quality_flag_l4a"] == 1)
        & (gdf_shots["agbd_l4a"] > 0)
        & gdf_shots["agbd_l4a"].notna()
    )
    shots_q = gdf_shots.loc[mask, ["agbd_l4a", "lon_lowestmode_l2a", "lat_lowestmode_l2a"]].copy()

    shots_egi = egi_dataframe(
        shots_q,
        x_col="lon_lowestmode_l2a",
        y_col="lat_lowestmode_l2a",
        level=6,
    )
    egi_agg = egi_agg_fn(shots_egi, mapper="mean", return_geometry=True)

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


# ─── Figure 3: HDF5 hierarchy vs H3 parquet layout ───────────────────────────
def fig_hdf5_vs_h3(out):
    """Conceptual diagram: nested HDF5 structure → flat H3 database."""
    setup()
    fig, axes = plt.subplots(1, 3, figsize=(10, 4.5), gridspec_kw={"width_ratios": [2, 0.4, 2]})
    ax_left, ax_mid, ax_right = axes

    for ax in axes:
        ax.set_xlim(0, 1)
        ax.set_ylim(0, 1)
        ax.axis("off")

    # ── Left: HDF5 nested hierarchy ──
    def hdf5_box(ax, x, y, w, h, label, color, fontsize=7):
        rect = mpatches.FancyBboxPatch(
            (x, y), w, h,
            boxstyle="round,pad=0.01",
            facecolor=color,
            edgecolor="#aaaaaa",
            lw=0.5,
        )
        ax.add_patch(rect)
        ax.text(x + w / 2, y + h / 2, label, ha="center", va="center",
                fontsize=fontsize, color="#222222")

    # Year
    hdf5_box(ax_left, 0.05, 0.82, 0.90, 0.13, "Year  (2019, 2020, … 2025)", "#dbeafe", 7.5)
    # DOY
    hdf5_box(ax_left, 0.12, 0.65, 0.76, 0.13, "Day-of-year  (001, 002, …)", "#dbeafe", 7.5)
    # Granule file
    hdf5_box(ax_left, 0.18, 0.48, 0.64, 0.13, "GEDI_L4A_…orbit….h5", "#bfdbfe", 7.5)
    # Beam
    hdf5_box(ax_left, 0.24, 0.31, 0.52, 0.13, "BEAM0000  ···  BEAM0110  (8 beams)", "#93c5fd", 7)
    # Variables
    hdf5_box(ax_left, 0.30, 0.13, 0.40, 0.13, "agbd  lat  lon  rh  …  (100s vars)", "#60a5fa", 7)

    # Indent lines
    for xs, ys, xe, ye in [
        (0.19, 0.82, 0.19, 0.78), (0.19, 0.78, 0.22, 0.78),
        (0.26, 0.65, 0.26, 0.61), (0.26, 0.61, 0.29, 0.61),
        (0.32, 0.48, 0.32, 0.44), (0.32, 0.44, 0.35, 0.44),
        (0.38, 0.31, 0.38, 0.27), (0.38, 0.27, 0.41, 0.27),
    ]:
        ax_left.plot([xs, xe], [ys, ye], color="#9ca3af", lw=0.5)

    ax_left.text(0.5, 0.98, "Raw GEDI (HDF5)", ha="center", va="top",
                 fontsize=9, fontweight="bold", color="#1e3a5f")
    ax_left.text(0.5, 0.03, "Orbit-organised  ·  thousands of large files\n"
                 "Spatial queries scan every file",
                 ha="center", va="bottom", fontsize=6.5, color="#555", style="italic")

    # ── Middle: arrow + label ──
    ax_mid.annotate(
        "",
        xy=(0.85, 0.5), xytext=(0.15, 0.5),
        arrowprops=dict(arrowstyle="-|>", color="#374151", lw=1.5),
    )
    ax_mid.text(0.5, 0.60, "gh3_build", ha="center", va="bottom",
                fontsize=7, fontweight="bold", color="#374151")

    # ── Right: H3 parquet layout ──
    def h3_box(ax, x, y, w, h, label, color, fontsize=7):
        rect = mpatches.FancyBboxPatch(
            (x, y), w, h,
            boxstyle="round,pad=0.01",
            facecolor=color,
            edgecolor="#aaaaaa",
            lw=0.5,
        )
        ax.add_patch(rect)
        ax.text(x + w / 2, y + h / 2, label, ha="center", va="center",
                fontsize=fontsize, color="#222222", family="monospace")

    # Database root
    h3_box(ax_right, 0.05, 0.82, 0.90, 0.13, "h3_database/", "#d1fae5", 8)
    # Build log
    h3_box(ax_right, 0.12, 0.65, 0.76, 0.11, "gedih3_build_log.json", "#a7f3d0", 7.5)
    # Partition tiles
    for i, (hex_id, y0) in enumerate([
        ("h3_03=838041fffffffff", 0.47),
        ("h3_03=83804cfffffffff", 0.32),
        ("h3_03=83804efffffffff", 0.17),
    ]):
        h3_box(ax_right, 0.12, y0, 0.76, 0.11, hex_id + "/", "#6ee7b7", 7)
        # parquet inside
        h3_box(ax_right, 0.22, y0 - 0.10, 0.56, 0.09, "data.parquet", "#34d399", 6.5)
        ax_right.plot([0.25, 0.25], [y0, y0 - 0.01], color="#9ca3af", lw=0.5)

    ax_right.text(0.5, 0.98, "gedih3 H3 Database", ha="center", va="top",
                  fontsize=9, fontweight="bold", color="#064e3b")
    ax_right.text(0.5, 0.03, "Spatially partitioned  ·  4 files for 1°×1°\n"
                  "Regional queries read only relevant tiles",
                  ha="center", va="bottom", fontsize=6.5, color="#555", style="italic")

    plt.tight_layout(pad=0.2)
    save_fig(fig, "hdf5_vs_h3.png", out)


# ─── Figure 4: H3 multi-resolution ───────────────────────────────────────────
def fig_h3_multi_resolution(gdf, out):
    """Three-panel: H3 hexagons at levels 3, 6, 9 over the study area."""
    setup()
    fig, axes = plt.subplots(1, 3, figsize=(10, 3.8))

    region_geojson = mapping(box(*BBOX))
    area_labels = {3: "~12,400 km²", 6: "~36 km²", 9: "~0.1 km²"}

    shots_sub = gdf.sample(min(5000, len(gdf)), random_state=42) if len(gdf) > 5000 else gdf

    for ax, res in zip(axes, [3, 6, 9]):
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

        ax.set_xlim(BBOX[0], BBOX[2])
        ax.set_ylim(BBOX[1], BBOX[3])
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
def fig_h3_two_level(gdf, out):
    """One level-3 partition filled with level-9 children + shot dots."""
    setup()
    fig, ax = plt.subplots(figsize=(4.5, 4.5))

    part_id = "838041fffffffff"
    part_poly = h3_cell_poly(part_id)
    part_gdf = gpd.GeoDataFrame(geometry=[part_poly], crs=4326)

    # Level-9 children (use geo_to_cells within the partition polygon)
    children = list(h3.geo_to_cells(mapping(part_poly), 9))
    children_gdf = cells_to_gdf(children)

    children_gdf.plot(
        ax=ax,
        facecolor="#dbeafe",
        edgecolor="#93c5fd",
        linewidth=0.2,
        alpha=0.7,
    )
    part_gdf.boundary.plot(ax=ax, color=HEX_COLOR, linewidth=1.8, zorder=5)

    # Shot dots (within partition)
    shots_in = gdf[
        (gdf["lon_lowestmode_l2a"] >= part_poly.bounds[0])
        & (gdf["lon_lowestmode_l2a"] <= part_poly.bounds[2])
        & (gdf["lat_lowestmode_l2a"] >= part_poly.bounds[1])
        & (gdf["lat_lowestmode_l2a"] <= part_poly.bounds[3])
    ]
    shots_sub = shots_in.sample(min(3000, len(shots_in)), random_state=0) if len(shots_in) > 3000 else shots_in
    ax.scatter(
        shots_sub["lon_lowestmode_l2a"],
        shots_sub["lat_lowestmode_l2a"],
        s=0.3,
        color=SHOT_COLOR,
        alpha=0.5,
        zorder=6,
        rasterized=True,
    )

    # Annotations
    cx, cy = part_poly.centroid.x, part_poly.centroid.y
    ax.text(
        cx, part_poly.bounds[3] + 0.15,
        "Partition tile  (H3 level 3,  ~12,400 km²)",
        ha="center", va="bottom",
        fontsize=7.5, color=HEX_COLOR, fontweight="bold",
    )
    ax.text(
        cx, cy,
        "Index cells\n(H3 level 12, ~307 m²)\n+ GEDI shots",
        ha="center", va="center",
        fontsize=7, color="#1e40af", alpha=0.85,
    )

    ax.set_aspect("equal")
    ax.set_xlabel("Longitude (°)")
    ax.set_ylabel("Latitude (°)")
    ax.set_title("H3 dual-level structure")
    hide_spines(ax)

    save_fig(fig, "h3_two_level.png", out)


# ─── Figure 6: H3 boundary / parent-child nesting ────────────────────────────
def fig_h3_boundary(out):
    """Show two adjacent level-5 hexes, children colored by assigned parent."""
    setup()
    fig, ax = plt.subplots(figsize=(4.5, 4.5))

    # Pick a level-5 hex inside the study region
    center_lat = (BBOX[1] + BBOX[3]) / 2
    center_lon = (BBOX[0] + BBOX[2]) / 2
    seed_cell = h3.latlng_to_cell(center_lat, center_lon, 5)
    neighbors = list(h3.grid_disk(seed_cell, 1))
    # Pick the seed and one neighbor
    neighbor = [n for n in neighbors if n != seed_cell][0]
    parent_pair = [seed_cell, neighbor]

    colors = {"A": "#dbeafe", "B": "#fef3c7"}
    edge_colors = {"A": HEX_COLOR, "B": "#d97706"}
    label_map = {seed_cell: "A", neighbor: "B"}

    # Children at level 10
    child_level = 10
    for p_id in parent_pair:
        children = list(h3.geo_to_cells(mapping(h3_cell_poly(p_id)), child_level))
        for c in children:
            assigned_parent = h3.cell_to_parent(c, 5)
            lbl = label_map.get(assigned_parent, "A")
            poly = h3_cell_poly(c)
            ax.fill(
                *poly.exterior.xy,
                color=colors[lbl],
                alpha=0.7,
                lw=0,
            )
            ax.plot(*poly.exterior.xy, color=edge_colors[lbl], lw=0.1, alpha=0.5)

    # Draw parent boundaries on top
    for p_id in parent_pair:
        poly = h3_cell_poly(p_id)
        lbl = label_map[p_id]
        ax.plot(*poly.exterior.xy, color=edge_colors[lbl], lw=1.5, zorder=5)
        cx, cy = poly.centroid.x, poly.centroid.y
        ax.text(cx, cy, f"Cell {lbl}", ha="center", va="center",
                fontsize=8, color=edge_colors[lbl], fontweight="bold", zorder=6)

    ax.set_aspect("equal")
    ax.set_xlabel("Longitude (°)")
    ax.set_ylabel("Latitude (°)")
    ax.set_title(
        "H3 parent/child nesting\nChildren near the boundary may be\nassigned to the non-enclosing parent",
        fontsize=8,
    )
    legend_handles = [
        mpatches.Patch(facecolor=colors["A"], edgecolor=edge_colors["A"], label="Assigned to cell A"),
        mpatches.Patch(facecolor=colors["B"], edgecolor=edge_colors["B"], label="Assigned to cell B"),
    ]
    ax.legend(handles=legend_handles, fontsize=7, frameon=False, loc="upper right")
    hide_spines(ax)

    save_fig(fig, "h3_boundary.png", out)


# ─── Figure 7: H3 hexagons vs EGI square pixels ──────────────────────────────
def fig_h3_vs_egi(h3_agg, egi_agg, out):
    """Side-by-side: H3 level-7 AGBD hexagons and EGI level-6 AGBD squares."""
    setup()
    fig, (ax1, ax2) = plt.subplots(1, 2, figsize=(8, 4))

    col_h3 = "agbd_l4a"
    col_egi = "agbd_l4a"

    h3_vals = h3_agg[col_h3].dropna()
    egi_vals = egi_agg[col_egi].dropna()
    vmin = min(h3_vals.quantile(0.05), egi_vals.quantile(0.05))
    vmax = max(h3_vals.quantile(0.95), egi_vals.quantile(0.95))

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
    ax1.set_title("H3 hexagons  (level 7, ~5 km²)\n", fontsize=8)
    ax1.set_xlabel("Longitude (°)")
    ax1.set_ylabel("Latitude (°)")
    hide_spines(ax1)

    # EGI panel
    egi_plot = egi_agg.to_crs(4326) if egi_agg.crs and egi_agg.crs.to_epsg() != 4326 else egi_agg
    sm = plt.cm.ScalarMappable(
        cmap=CMAP_AGBD,
        norm=plt.Normalize(vmin=vmin, vmax=vmax),
    )
    sm.set_array([])
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
    ax2.set_title("EGI square pixels  (level 6, ~1 km)\n", fontsize=8)
    ax2.set_xlabel("Longitude (°)")
    ax2.set_yticklabels([])
    hide_spines(ax2)

    cbar = fig.colorbar(sm, ax=[ax1, ax2], shrink=0.7, pad=0.02)
    cbar.set_label("Mean AGBD (Mg ha⁻¹)", fontsize=7)
    cbar.ax.tick_params(labelsize=6)

    plt.suptitle("H3 hexagons vs EGI square pixels — same AGBD data", fontsize=9, y=1.01)
    plt.tight_layout()
    save_fig(fig, "h3_vs_egi.png", out)


# ─── Figure 8: EGI AGBD map ───────────────────────────────────────────────────
def fig_egi_agbd_map(egi_agg, out):
    """Single-panel EGI level-6 AGBD map."""
    setup()
    fig, ax = plt.subplots(figsize=(5, 4.5))

    col = "agbd_l4a"
    egi_plot = egi_agg.to_crs(4326) if egi_agg.crs and egi_agg.crs.to_epsg() != 4326 else egi_agg
    vmin = egi_agg[col].quantile(0.05)
    vmax = egi_agg[col].quantile(0.95)

    sm = plt.cm.ScalarMappable(
        cmap=CMAP_AGBD,
        norm=plt.Normalize(vmin=vmin, vmax=vmax),
    )
    sm.set_array([])
    egi_plot.plot(
        column=col,
        ax=ax,
        cmap=CMAP_AGBD,
        vmin=vmin,
        vmax=vmax,
        edgecolor="none",
        legend=False,
        missing_kwds={"color": "#f3f4f6"},
    )

    cbar = fig.colorbar(sm, ax=ax, shrink=0.85, pad=0.02)
    cbar.set_label("Mean AGBD (Mg ha⁻¹)", fontsize=7)
    cbar.ax.tick_params(labelsize=6)

    ax.set_xlabel("Longitude (°)")
    ax.set_ylabel("Latitude (°)")
    ax.set_title("EGI level-6 (~1 km) aggregated AGBD", fontsize=9)
    hide_spines(ax)

    save_fig(fig, "egi_agbd_map.png", out)


# ─── Figure 9: Per-hexagon regression R² ─────────────────────────────────────
def fig_regression_r2(gdf_shots, out):
    """Per-hexagon R² of AGBD ~ canopy height (rh_098) at H3 level 7.

    Uses pandas directly (avoids Dask metadata mismatch with custom callables).
    """
    setup()

    from sklearn.linear_model import LinearRegression
    from sklearn.metrics import r2_score

    # Quality filter
    mask = (
        (gdf_shots["l4_quality_flag_l4a"] == 1)
        & (gdf_shots["quality_flag_l2a"] == 1)
        & gdf_shots["agbd_l4a"].notna()
        & gdf_shots["rh_098_l2a"].notna()
        & (gdf_shots["agbd_l4a"] > 0)
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

    # Add H3 level-7 parent for each shot (vectorized list comprehension)
    shots_q["h3_07"] = [h3.cell_to_parent(c, 7) for c in shots_q["h3_12"]]

    # Regression per hexagon
    def fit_per_hex(df):
        n = len(df)
        if n < 5:
            return pd.Series({"r2": np.nan, "n": n})
        X = df["rh_098_l2a"].values.reshape(-1, 1)
        y = df["agbd_l4a"].values
        model = LinearRegression().fit(X, y)
        r2 = r2_score(y, model.predict(X))
        return pd.Series({"r2": float(max(0.0, r2)), "n": n})

    results = shots_q.groupby("h3_07").apply(fit_per_hex, include_groups=False).reset_index()
    results = results[results["n"] >= 5].dropna(subset=["r2"])

    if len(results) == 0:
        print("  [skip] regression_r2.png — insufficient data")
        return

    # Add polygon geometry
    results["geometry"] = [h3_cell_poly(c) for c in results["h3_07"]]
    results = gpd.GeoDataFrame(results, geometry="geometry", crs=4326)

    fig, ax = plt.subplots(figsize=(5, 4.5))

    sm = plt.cm.ScalarMappable(
        cmap=CMAP_R2,
        norm=plt.Normalize(vmin=0, vmax=1),
    )
    sm.set_array([])
    results.plot(
        column="r2",
        ax=ax,
        cmap=CMAP_R2,
        vmin=0,
        vmax=1,
        edgecolor="none",
        legend=False,
        missing_kwds={"color": "#f3f4f6"},
    )

    cbar = fig.colorbar(sm, ax=ax, shrink=0.85, pad=0.02)
    cbar.set_label("R²  (AGBD ~ canopy height)", fontsize=7)
    cbar.ax.tick_params(labelsize=6)

    ax.set_xlabel("Longitude (°)")
    ax.set_ylabel("Latitude (°)")
    ax.set_title(
        "Per-hexagon linear regression R²\nAGBD ~ RH98 canopy height  (H3 level 7)",
        fontsize=8,
    )
    hide_spines(ax)

    save_fig(fig, "regression_r2.png", out)


# ─── Main ─────────────────────────────────────────────────────────────────────
def parse_args():
    p = argparse.ArgumentParser(description="Generate gedih3 documentation illustrations.")
    p.add_argument("--db", default=DEFAULT_DB, help="Path to H3 database")
    p.add_argument("--out", default=DEFAULT_OUT, help="Output directory for PNGs")
    return p.parse_args()


def main():
    args = parse_args()
    out = Path(args.out)
    out.mkdir(parents=True, exist_ok=True)

    print(f"Database : {args.db}")
    print(f"Output   : {out.resolve()}")
    print()

    # ── Conceptual figures (no data needed) ──────────────────────────────────
    print("[1/9] lidar_waveform.png  (conceptual)")
    fig_lidar_waveform(out)

    print("[2/9] hdf5_vs_h3.png  (conceptual)")
    fig_hdf5_vs_h3(out)

    print("[3/9] h3_boundary.png  (H3 library only)")
    fig_h3_boundary(out)

    # ── Load data once ────────────────────────────────────────────────────────
    print("\nLoading shots from database…")
    gdf = load_shots(args.db)
    print(f"  {len(gdf):,} shots loaded")

    print("\n[4/9] gedi_tracks.png")
    fig_gedi_tracks(gdf, out)

    print("[5/9] h3_multi_resolution.png")
    fig_h3_multi_resolution(gdf, out)

    print("[6/9] h3_two_level.png")
    fig_h3_two_level(gdf, out)

    # ── Aggregated data ───────────────────────────────────────────────────────
    print("\nAggregating data (H3 level 7 + EGI level 6)…")
    h3_agg, egi_agg = load_aggregated(args.db, gdf)
    print(f"  H3 agg: {len(h3_agg):,} hexes  |  EGI agg: {len(egi_agg):,} pixels")

    print("\n[7/9] h3_vs_egi.png")
    fig_h3_vs_egi(h3_agg, egi_agg, out)

    print("[8/9] egi_agbd_map.png")
    fig_egi_agbd_map(egi_agg, out)

    # ── Regression ────────────────────────────────────────────────────────────
    print("[9/9] regression_r2.png")
    fig_regression_r2(gdf, out)

    print(f"\nDone — {len(list(out.glob('*.png')))} PNGs in {out.resolve()}")


if __name__ == "__main__":
    main()
