# Configuration file for the Sphinx documentation builder.
#
# This file only contains a selection of the most common options. For a full
# list see the documentation:
# https://www.sphinx-doc.org/en/master/usage/configuration.html

# -- Path setup --------------------------------------------------------------

# If extensions (or modules to document with autodoc) are in another directory,
# add these directories to sys.path here. If the directory is relative to the
# documentation root, use os.path.abspath to make it absolute, like shown here.
#
import os
import sys

sys.path.insert(0, os.path.abspath("../"))

# -- Project information -----------------------------------------------------

project = "pyexasol"
copyright = "2024, Exasol"  # pylint: disable=redefined-builtin
author = "Exasol"

# -- General configuration ---------------------------------------------------

# Add any Sphinx extension module names here, as strings. They can be
# extensions coming with Sphinx (named 'sphinx.ext.*') or your custom
# ones.
extensions = [
    "sphinx.ext.todo",
    "sphinx.ext.autodoc",
    "sphinx.ext.viewcode",
    "sphinx.ext.napoleon",
    "sphinx.ext.intersphinx",
    "sphinx.ext.autosummary",
    "sphinx_copybutton",
    "myst_parser",
    "sphinx_design",
    "exasol.toolbox.sphinx.multiversion",
]

intersphinx_mapping = {"python": ("https://docs.python.org/3", None)}

# Make sure the target is unique
source_suffix = {
    ".rst": "restructuredtext",
    ".txt": "markdown",
    ".md": "markdown",
}

todo_include_todos = True

# Add any paths that contain templates here, relative to this directory.
templates_path = ["_templates"]

# List of patterns, relative to source directory, that match files and
# directories to ignore when looking for source files.
# This pattern also affects html_static_path and html_extra_path.
exclude_patterns = ["_build", "Thumbs.db", ".DS_Store", ".build-docu"]

# -- Options for HTML output -------------------------------------------------

# The theme to use for HTML and HTML Help pages.  See the documentation for
# a list of builtin themes.
#
html_theme = "shibuya"

# Add any paths that contain custom static files (such as style sheets) here,
# relative to this directory. They are copied after the builtin static files,
# so a file named "default.css" will overwrite the builtin "default.css".
html_static_path = ["_static"]
html_title = "pyexasol"
html_theme_options = {
    "light_logo": "_static/light-exasol-logo.svg",
    "dark_logo": "_static/dark-exasol-logo.svg",
    "github_url": "https://github.com/exasol/pyexasol",
    "accent_color": "grass",
}
# -- Configure link checking behavior  ----------------------------------------
linkcheck_rate_limit_timeout = 60
linkcheck_timeout = 30
linkcheck_delay = 30
linkcheck_retries = 2
linkcheck_anchors = False
linkcheck_ignore: list[str] = []
linkcheck_allowed_redirects = {
    # All HTTP redirections from the source URI to
    # the canonical URI will be treated as "working".
    r"https://github\.com/.*": r"https://github\.com/login*",
    r"https://exasol\.my\.site\.com/s/article/.*": r"https://exasol\.my\.site\.com/s/article/.*?language=en_US",
}
