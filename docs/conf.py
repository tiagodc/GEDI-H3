import os
import sys

# -- Project information -----------------------------------------------------
project = "gedih3"
copyright = "2026, University of Maryland. All Rights Reserved"
author = "Tiago de Conto and Amelia Grace Holcomb"
release = "0.12.2"

# -- General configuration ---------------------------------------------------
extensions = [
    "sphinx.ext.autodoc",
    "sphinx.ext.autosummary",
    "numpydoc",
    "myst_parser",
    "autoapi.extension",
    "sphinx.ext.viewcode",
    "sphinx.ext.intersphinx",
    "sphinx_design",
    "sphinxcontrib.mermaid",
]

# sphinx-autoapi: generates API docs without importing the package
autoapi_dirs = ["../src/gedih3"]
autoapi_type = "python"
# Skip the package data dir — it ships .py runtime assets (e.g. the dask
# worker preload `dask-worker-trim.py`) that aren't part of the public API and
# whose docstrings legitimately contain trailing-underscore tokens like
# MALLOC_TRIM_THRESHOLD_ that autoapi mis-parses as RST hyperlink references.
autoapi_ignore = ["*/data/*"]
autoapi_options = [
    "members",
    "undoc-members",
    "show-inheritance",
    "special-members",
    "show-module-summary",
]
autoapi_add_toctree_entry = True
autoapi_keep_files = False

# numpydoc
numpydoc_show_class_members = False
numpydoc_class_members_toctree = False
numpydoc_additional_section_headers = ['Key Features', 'Basic Usage', 'Resolution Levels']

# myst-parser: allow Markdown files in toctree
myst_enable_extensions = ["colon_fence"]
myst_heading_anchors = 0
suppress_warnings = ["myst.header", "intersphinx.remote"]
source_suffix = {
    ".rst": "restructuredtext",
    ".md": "markdown",
}

# intersphinx: cross-reference to other projects
intersphinx_timeout = 10  # seconds — fail fast on unreachable servers
intersphinx_mapping = {
    "python": ("https://docs.python.org/3", None),
    "numpy": ("https://numpy.org/doc/stable", None),
    "pandas": ("https://pandas.pydata.org/docs", None),
    "geopandas": ("https://geopandas.org/en/stable", None),
    "dask": ("https://docs.dask.org/en/stable", None),
}

# -- HTML output -------------------------------------------------------------
html_theme = "pydata_sphinx_theme"
html_static_path = ["imgs", "_static"]
html_css_files = ["custom.css"]
html_theme_options = {
    "logo": {
        "image_light": "gh3_logo.png",
        "image_dark": "gh3_logo.png",
        "link": "/GEDI-H3/",
    },
    "github_url": "https://github.com/tiagodc/GEDI-H3",
    "use_edit_page_button": False,
    "show_toc_level": 2,
    "navigation_with_keys": True,
    "show_nav_level": 1,
}

html_sidebars = {
    "**": ["sidebar-nav-bs"],
}

# Show full navigation tree in the left sidebar
navigation_depth = 4

templates_path = ["_templates"]
exclude_patterns = ["_build", "Thumbs.db", ".DS_Store"]

# -- Autodoc -----------------------------------------------------------------
autodoc_default_options = {
    "members": True,
    "undoc-members": False,
    "show-inheritance": True,
}
