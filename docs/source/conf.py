# -*- coding: utf-8 -*-

# urbanaccess documentation build configuration file, created by
# sphinx-quickstart on Tue March 14 13:35:40 2017.

import sys
import os
import sphinx_rtd_theme
from datetime import datetime

# -- General configuration ------------------------------------------------

# If your documentation needs a minimal Sphinx version, state it here.
#
# needs_sphinx = '1.0'
numpydoc_show_class_members = False
numpydoc_class_members_toctree = False

# Sphinx extensions
extensions = [
    'sphinx.ext.autodoc',
    'sphinx.ext.mathjax',
    'numpydoc',
    'sphinx.ext.autosummary'
]

templates_path = ['_templates']
source_suffix = '.rst'
master_doc = 'index'
project = u'UrbanAccess'
author = u'UrbanSim Inc.'
copyright = u'{}, {}'.format(datetime.now().year, author)
version = u'0.2.0'
release = u'0.2.0'
language = None

# List of patterns to ignore when looking for source files.
exclude_patterns = ['_build']

# Pygments (syntax highlighting) style
pygments_style = 'sphinx'

todo_include_todos = False

# -- Options for HTML output ----------------------------------------------
html_theme = 'sphinx_rtd_theme'
html_theme_path = [sphinx_rtd_theme.get_html_theme_path()]
# html_theme_options = {}

# paths that contain custom static files (such as style sheets)
html_static_path = ['_static']

# -- Options for HTMLHelp output ------------------------------------------
# Output file base name for HTML help builder.
htmlhelp_basename = 'UrbanAccessdoc'
html_show_sphinx = False
html_show_sourcelink = True

# -- Options for LaTeX output ---------------------------------------------
latex_elements = {
}

# Grouping the document tree into LaTeX files. List of tuples
# (source start file, target name, title,
#  author, documentclass [howto, manual, or own class]).
latex_documents = [
    (master_doc, 'UrbanAccess.tex', u'UrbanAccess Documentation',
     u'UrbanSim Inc.', 'manual'),
]

# -- Options for manual page output ---------------------------------------
# One entry per manual page. List of tuples
# (source start file, name, description, authors, manual section).
man_pages = [
    (master_doc, 'UrbanAccess', u'UrbanAccess Documentation',
     [author], 1)
]

# -- Options for Texinfo output -------------------------------------------
# Grouping the document tree into Texinfo files. List of tuples
# (source start file, target name, title, author,
#  dir menu entry, description, category)
texinfo_documents = [
    (master_doc, 'UrbanAccess', u'UrbanAccess Documentation',
     author, 'UrbanAccess', 'One line description of project.',
     'Miscellaneous'),
]

# Example configuration for intersphinx: refer to the Python standard library.
# intersphinx_mapping = {'https://docs.python.org/': None}
