#!/usr/bin/env python3
"""
generate_illustrations.py — Generate gedih3 documentation figures.

Figures produced:
  1. gedi_tracks.png          — GEDI orbital track pattern (real data)
  2. lidar_waveform.png       — Full-waveform LiDAR schematic (synthetic)
  3. h3_multi_resolution.png  — H3 hexagons at levels 3, 6, 9 (real data)
  4. h3_two_level.png         — H3 dual-level partition+index structure
  5. h3_boundary.png          — H3 parent/child boundary nesting caveat
  6. h3_vs_egi.png            — H3 hexagons vs EGI square pixels (real data)
  7. egi_agbd_map.png         — EGI-aggregated AGBD map (real data)
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
    print("[1/8] lidar_waveform.png  (conceptual)")
    fig_lidar_waveform(out)

    print("[2/8] h3_boundary.png  (H3 library only)")
    fig_h3_boundary(out)

    # ── Load data once ────────────────────────────────────────────────────────
    print("\nLoading shots from database…")
    gdf = load_shots(args.db)
    print(f"  {len(gdf):,} shots loaded")

    print("\n[3/8] gedi_tracks.png")
    fig_gedi_tracks(gdf, out)

    print("[4/8] h3_multi_resolution.png")
    fig_h3_multi_resolution(gdf, out)

    print("[5/8] h3_two_level.png")
    fig_h3_two_level(args.db, out)

    # ── Aggregated data ───────────────────────────────────────────────────────
    print("\nAggregating data (H3 level 7 + EGI level 6)…")
    h3_agg, egi_agg = load_aggregated(args.db, gdf)
    print(f"  H3 agg: {len(h3_agg):,} hexes  |  EGI agg: {len(egi_agg):,} pixels")

    print("\n[6/8] h3_vs_egi.png")
    fig_h3_vs_egi(h3_agg, egi_agg, out)

    print("[7/8] egi_agbd_map.png")
    fig_egi_agbd_map(egi_agg, out)

    # ── Regression ────────────────────────────────────────────────────────────
    print("[8/8] regression_r2.png")
    fig_regression_r2(gdf, out)

    print(f"\nDone — {len(list(out.glob('*.png')))} PNGs in {out.resolve()}")


if __name__ == "__main__":
    main()
