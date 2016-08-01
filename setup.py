from setuptools import setup
import os
from urllib import urlretrieve



def get_long_description():
    readme_file = 'README.md'
    if not os.path.isfile(readme_file):
        return ''
    # Try to transform the README from Markdown to reStructuredText.
    try:
        import pandoc
        pandoc.core.PANDOC_PATH = 'pandoc'
        doc = pandoc.Document()
        doc.markdown = open(readme_file).read()
        description = doc.rst
    except Exception:
        description = open(readme_file).read()
    return description









setup(
	name='drm4g'
	version='2.0', # or the method found in netcdf's implementation could work 'calculate_version'
    description='A Pythonic framework for working with simulations',
    author='Meteorology Group UC',
    author_email='josecarlos.blanco@unican.es',
    url='https://meteo.unican.es/gitbucket/git/DRM4G/DRM4G.git',
    license='GNU Affero General Public License',
    description='Placeholder.',
    long_description = get_long_description(),
    classifiers=[
        "Intended Audience :: Science/Research",
        "Programming Language :: Python",
        "Topic :: Scientific/Engineering,
        "Topic :: Office/Business :: Scheduling",
    ],
)