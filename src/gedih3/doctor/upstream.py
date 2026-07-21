# Copyright (C) 2026, University of Maryland. All Rights Reserved.
# Authors: Tiago de Conto, Amelia Grace Holcomb
# For commercial licensing inquiries, contact UM Ventures at umdtechtransfer@umd.edu

"""Upstream NASA CMR check + backfill/build recommendation engine.

Triggered by the ``--online`` flag. Queries NASA CMR (via the existing
:class:`gedih3.daac.GEDIAccessor`) for each product the database tracks, in
the database's own spatial/temporal extent, and classifies each gap into one
of five buckets:

  - ``BACKFILL_LOCAL``   : granule indexed in DB; missing product file is on disk
  - ``BACKFILL_REMOTE``  : granule indexed; missing file on NASA, not local
  - ``BACKFILL_UNAVAIL`` : granule indexed; NASA does not list the missing product
  - ``BUILD_NEW``        : granule available on NASA in DB extent; not indexed
  - ``OUT_OF_SCOPE``     : granule available on NASA outside DB extent

The classifier maps each bucket to a concrete CLI command the user can run.
The doctor never executes these commands.
"""

from __future__ import annotations

import logging
import os
from collections import defaultdict
from dataclasses import dataclass, field
from typing import Dict, List, Optional, Set, Tuple

GranuleKey = Tuple[int, int, int]


@dataclass
class UpstreamReport:
    """Summary of NASA-side granule availability vs database state."""
    available_per_product: Dict[str, Set[GranuleKey]] = field(default_factory=dict)
    db_granule_keys: Set[GranuleKey] = field(default_factory=set)
    db_per_product: Dict[str, Set[GranuleKey]] = field(default_factory=dict)
    classifications: Dict[str, List[Tuple[GranuleKey, str]]] = field(default_factory=dict)
    recommendations: List[str] = field(default_factory=list)

    def to_dict(self):
        return {
            'available_per_product': {p: sorted(s) for p, s in self.available_per_product.items()},
            'db_granule_keys': sorted(self.db_granule_keys),
            'db_per_product': {p: sorted(s) for p, s in self.db_per_product.items()},
            'classifications': {
                cls: [{'orbit': k[0], 'granule': k[1], 'track': k[2], 'product': p} for k, p in items]
                for cls, items in self.classifications.items()
            },
            'recommendations': self.recommendations,
        }


def _extract_granule_keys(granules) -> Set[GranuleKey]:
    """Parse earthaccess granule objects into ``(orbit, granule, track)`` keys."""
    from ..gedidriver import GEDIFile
    out = set()
    for g in granules:
        try:
            data_link = g.data_links()[0]
            gf = GEDIFile(data_link)
            out.add((gf.orbit, gf.orbit_granule, gf.track))
        except Exception:
            continue
    return out


def query_available_granules(
    spatial, temporal, products: List[str], version=None, logger=None,
) -> Dict[str, Set[GranuleKey]]:
    """For each product, query NASA CMR and return granule keys."""
    from ..daac import GEDIAccessor
    log = logger or logging.getLogger(__name__)

    if not products:
        return {}

    accessor = GEDIAccessor(authenticate=True, spatial=spatial, temporal=temporal)
    out: Dict[str, Set[GranuleKey]] = {}
    for prod in products:
        try:
            accessor.search_data(product=prod, version=version)
            out[prod] = _extract_granule_keys(accessor.product_files.get(prod.upper(), []))
            log.info(f"  CMR {prod}: {len(out[prod])} granules in DB extent")
        except Exception as e:
            log.warning(f"CMR query for {prod} failed: {e}")
            out[prod] = set()
    return out


def _local_soc_keys_for_product(soc_dir: Optional[str], product: str) -> Set[GranuleKey]:
    """Return granule keys for a product whose source HDF5 is on disk."""
    if not soc_dir or not os.path.isdir(soc_dir):
        return set()
    from ..gh3builder import soc_file_tree
    tree = soc_file_tree(soc_dir, to_list=False)
    out = set()
    for orb_track, files in tree.items():
        if product not in files:
            continue
        # Parse "Oxxxxx_xx_Txxxxx" → (orbit, granule, track)
        try:
            parts = orb_track.split('_')
            o = int(parts[0][1:])
            g = int(parts[1])
            t = int(parts[2][1:])
            out.add((o, g, t))
        except (IndexError, ValueError):
            continue
    return out


def _classify(
    db_granules: Set[GranuleKey],
    db_per_product: Dict[str, Set[GranuleKey]],
    available_per_product: Dict[str, Set[GranuleKey]],
    local_per_product: Dict[str, Set[GranuleKey]],
    products: List[str],
) -> Dict[str, List[Tuple[GranuleKey, str]]]:
    """Bucket each (granule, product) into one of the five classes."""
    classes = defaultdict(list)

    for prod in products:
        # Granules indexed in DB but missing this product:
        missing = db_granules - db_per_product.get(prod, set())
        avail = available_per_product.get(prod, set())
        local = local_per_product.get(prod, set())
        for k in sorted(missing):
            if k in local:
                classes['BACKFILL_LOCAL'].append((k, prod))
            elif k in avail:
                classes['BACKFILL_REMOTE'].append((k, prod))
            else:
                classes['BACKFILL_UNAVAIL'].append((k, prod))

        # Granules NASA lists in the DB extent but not yet indexed:
        new_in_extent = avail - db_granules
        for k in sorted(new_in_extent):
            classes['BUILD_NEW'].append((k, prod))

    return dict(classes)


def _emit_recommendations(
    classes: Dict[str, List[Tuple[GranuleKey, str]]],
    db_path: str,
    soc_path: Optional[str],
) -> List[str]:
    recs = []

    # Group BACKFILL classes by product for compact suggestions.
    backfill_local = defaultdict(list)
    backfill_remote = defaultdict(list)
    build_new = defaultdict(list)
    for key, prod in classes.get('BACKFILL_LOCAL', []):
        backfill_local[prod].append(key)
    for key, prod in classes.get('BACKFILL_REMOTE', []):
        backfill_remote[prod].append(key)
    for key, prod in classes.get('BUILD_NEW', []):
        build_new[prod].append(key)

    if backfill_local:
        prods = ','.join(sorted(backfill_local))
        n = sum(len(v) for v in backfill_local.values())
        recs.append(
            f"# {n} (granule × product) gaps are fillable locally for {prods}:"
        )
        recs.append(f"gh3_doctor -i {db_path} --fix backfill")

    if backfill_remote:
        prods = ','.join(sorted(backfill_remote))
        n = sum(len(v) for v in backfill_remote.values())
        recs.append(
            f"# {n} (granule × product) gaps need NASA download for {prods}:"
        )
        if soc_path:
            flags = ' '.join(f"-{p.lower()} default" for p in sorted(backfill_remote))
            recs.append(
                f"gh3_download -i {soc_path} {flags}   # then: gh3_doctor -i {db_path} --fix backfill"
            )
        recs.append(f"# OR one-shot via S3 ETL (no persistent download):")
        recs.append(f"gh3_doctor -i {db_path} --fix backfill --s3")

    if build_new:
        prods = ','.join(sorted(build_new))
        n = sum(len(v) for v in build_new.values())
        recs.append(
            f"# {n} new granules in DB extent are available on NASA for {prods} (not yet indexed):"
        )
        target = soc_path or '$SOC'
        flags = ' '.join(f"-{p.lower()} default" for p in sorted(build_new))
        recs.append(
            f"gh3_build -i {target} -o {db_path} {flags}   # add new granules"
        )
        recs.append(
            f"# OR via S3 (no persistent local download):"
        )
        recs.append(
            f"gh3_build -o {db_path} {flags} --s3"
        )

    n_unavail = len(classes.get('BACKFILL_UNAVAIL', []))
    if n_unavail:
        recs.append(
            f"# {n_unavail} (granule × product) gaps remain — NASA does not currently list these. Nothing to do."
        )

    return recs


def gather_upstream(ctx, logger=None) -> UpstreamReport:
    """Run the upstream check and produce an :class:`UpstreamReport`.

    Reads the DB extent from the build log. Computes per-product availability
    on NASA, the per-product on-disk presence, and the per-product DB
    presence, then classifies and emits recommendations.
    """
    log = logger or logging.getLogger(__name__)

    if ctx.h3_logger is None or not ctx.h3_logger.product_vars:
        log.info("Upstream check skipped: no build log or product list")
        return UpstreamReport()

    products = sorted(ctx.h3_logger.product_vars.keys())
    spatial = getattr(ctx.h3_logger, 'spatial', None)
    temporal = getattr(ctx.h3_logger, 'temporal', None)
    version = getattr(ctx.h3_logger, 'gedi_version', None)

    log.info(f"Querying NASA CMR for {products} in DB extent...")
    available = query_available_granules(spatial, temporal, products,
                                         version=version, logger=log)

    # Build DB granule sets per product from the log's per-product status.
    db_granules: Set[GranuleKey] = set()
    db_per_product: Dict[str, Set[GranuleKey]] = defaultdict(set)
    for g in (ctx.h3_logger.granule_info or []):
        key = (g['orbit'], g['granule'], g['track'])
        db_granules.add(key)
        for prod, status in (g.get('products') or {}).items():
            if status == 'INDEXED':
                db_per_product[prod].add(key)

    local_per_product = {p: _local_soc_keys_for_product(ctx.soc_dir, p) for p in products}

    classes = _classify(db_granules, dict(db_per_product), available, local_per_product, products)
    recs = _emit_recommendations(classes, ctx.h3_dir, ctx.soc_dir)

    return UpstreamReport(
        available_per_product=available,
        db_granule_keys=db_granules,
        db_per_product=dict(db_per_product),
        classifications=classes,
        recommendations=recs,
    )
