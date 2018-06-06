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
    version='0.2',
    license='AGPL',
    description='A tool for creating GTFS transit and OSM pedestrian networks for use in Pandana accessibility analyses.',
    long_description=long_description,
    author='UrbanSim Inc. and Samuel D. Blanchard',
    url='https://github.com/UDST/urbanaccess',
    classifiers=[
        'Development Status :: 3 - Alpha',
        'Programming Language :: Python :: 2.7',
        'Programming Language :: Python :: 3.5',
        'License :: OSI Approved :: GNU Affero General Public License v3'
    ],
    packages=find_packages(exclude=['*.tests']),
    install_requires=[
        'requests>=2.9.1',
        'pandas>=0.20.3',
        'numpy>=1.13.1',
        'osmnet==0.1.4',
        'pandana>=0.4.0.1',
        'matplotlib>=2.0.2',
        'geopy>=1.11.0',
        'pyyaml>=3.11',
        'scikit-learn>=0.18.2'
    ]
)
