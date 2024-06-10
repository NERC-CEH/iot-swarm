# Configuration file for the Sphinx documentation builder.
#
# For the full list of built-in configuration values, see the documentation:
# https://www.sphinx-doc.org/en/master/usage/configuration.html

# -- Project information -----------------------------------------------------
# https://www.sphinx-doc.org/en/master/usage/configuration.html#project-information

import os, sys

path = os.path.abspath("../src")
sys.path.append(path)
print(path)
import iotswarm

project = "IoT Swarm"
copyright = "2024, Lewis Chambers"
author = "Lewis Chambers"
release = iotswarm.__version__
version = release
# -- General configuration ---------------------------------------------------
# https://www.sphinx-doc.org/en/master/usage/configuration.html#general-configuration

extensions = [
    "sphinx.ext.autodoc",
    "sphinx.ext.napoleon",
    "sphinx.ext.intersphinx",
    "sphinx_rtd_theme",
    "sphinx_copybutton",
    "sphinx_click",
]


napoleon_google_docstring = True
napoleon_use_param = False
napoleon_attr_attributes = True

intersphinx_mapping = {
    "python": ("https://docs.python.org/3", None),
    "oracledb": ("https://python-oracledb.readthedocs.io/en/latest", None),
}

templates_path = ["_templates"]
exclude_patterns = ["_build", "Thumbs.db", ".DS_Store"]

pygments_style = "sphinx"
autodoc_member_order = "bysource"

# -- Options for HTML output -------------------------------------------------
# https://www.sphinx-doc.org/en/master/usage/configuration.html#options-for-html-output

html_theme = "sphinx_rtd_theme"
html_static_path = ["_static"]
