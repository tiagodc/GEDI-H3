import os
import sys

# -- Project information -----------------------------------------------------
project = "gedih3"
copyright = "2024, Tiago de Conto"
author = "Tiago de Conto"
release = "0.0.1"

# -- General configuration ---------------------------------------------------
extensions = [
    "sphinx.ext.autodoc",
    "sphinx.ext.autosummary",
    "numpydoc",
    "myst_parser",
    "autoapi.extension",
    "sphinx.ext.viewcode",
    "sphinx.ext.intersphinx",
]

# sphinx-autoapi: generates API docs without importing the package
autoapi_dirs = ["../src/gedih3"]
autoapi_type = "python"
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
source_suffix = {
    ".rst": "restructuredtext",
    ".md": "markdown",
}

# intersphinx: cross-reference to other projects
intersphinx_mapping = {
    "python": ("https://docs.python.org/3", None),
    "numpy": ("https://numpy.org/doc/stable", None),
    "pandas": ("https://pandas.pydata.org/docs", None),
    "geopandas": ("https://geopandas.org/en/stable", None),
    "dask": ("https://docs.dask.org/en/stable", None),
}

# -- HTML output -------------------------------------------------------------
html_theme = "pydata_sphinx_theme"
html_theme_options = {
    "github_url": "https://github.com/tiagodc/gedih3",
    "use_edit_page_button": False,
    "show_toc_level": 2,
    "navigation_with_keys": True,
}

templates_path = ["_templates"]
exclude_patterns = ["_build", "Thumbs.db", ".DS_Store"]

# -- Autodoc -----------------------------------------------------------------
autodoc_default_options = {
    "members": True,
    "undoc-members": False,
    "show-inheritance": True,
}
