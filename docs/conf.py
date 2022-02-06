"""Sphinx configuration."""
project = "Pydantic Pynamodb"
author = "Andrew Herrington"
copyright = "2022, Andrew Herrington"
extensions = ["sphinx.ext.autodoc", "sphinx.ext.napoleon"]
autodoc_typehints = "description"
html_theme = "furo"
