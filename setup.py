from setuptools import setup, find_packages

# read README as the long description
with open('README.rst', 'r') as f:
    long_description = f.read()

description = ('A tool for creating GTFS transit and OSM pedestrian networks '
               'for use in Pandana accessibility analyses.')

with open("requirements.txt") as f:
    required_packages = [line.strip() for line in f.readlines()]

setup(
    name='urbanaccess',
    version='0.2.2',
    license='AGPL',
    description=description,
    long_description=long_description,
    author='UrbanSim Inc. and Samuel D. Blanchard',
    url='https://github.com/UDST/urbanaccess',
    classifiers=[
        'Programming Language :: Python :: 2',
        'Programming Language :: Python :: 2.7',
        'Programming Language :: Python :: 3',
        'Programming Language :: Python :: 3.5',
        'Programming Language :: Python :: 3.6',
        'Programming Language :: Python :: 3.7',
        'Programming Language :: Python :: 3.8',
        'Development Status :: 3 - Alpha',
        'License :: OSI Approved :: GNU Affero General Public License v3'
    ],
    packages=find_packages(exclude=['*.tests']),
    install_requires=[required_packages],
    extras_require={
        "NetworkX": ["networkx"]}
)
