# Install setuptools if not installed.
try:
    import setuptools
except ImportError:
    from ez_setup import use_setuptools
    use_setuptools()

from setuptools import setup, find_packages


# read README as the long description
with open('README.md', 'r') as f:
    long_description = f.read()

setup(
    name='urbanaccess',
    version='0.1a',
    license='AGPL',
    description='A tool for creating GTFS transit and OSM pedestrian networks for use in Pandana accessibility analyses.',
    long_description=long_description,
    author='UrbanSim Inc. and Samuel D. Blanchard',
    url='https://github.com/UDST/urbanaccess',
    classifiers=[
        'Programming Language :: Python :: 2.7',
        'Development Status :: 3 - Alpha'
        'License :: OSI Approved :: GNU Affero General Public License v3'
    ],
    packages=find_packages(exclude=['*.tests']),
    install_requires=[
        'requests >= 2.9.1',
        'pandas >= 0.16.0',
        'numpy >= 1.11'
        'osmnet>=0.1a',
        'pandana>=0.2.0',
        'matplotlib>=2.0',
        'urllib>=1.17',
        'urllib2>=2.7',
        'geopy>=1.11.0',
        'sklearn>=0.17.1'
    ]
)
