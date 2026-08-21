from setuptools import setup, find_packages

# read README as the long description
with open('README.rst', 'r') as f:
    long_description = f.read()

description = 'A tool for creating GTFS transit and OSM pedestrian networks ' \
              'for use in Pandana accessibility analyses.'

setup(
    name='urbanaccess',
    version='0.2.2',
    license='AGPL',
    description=description,
    long_description=long_description,
    author='UrbanSim Inc. and Samuel D. Blanchard',
    url='https://github.com/UDST/urbanaccess',
    python_requires='>=3.10,<3.12',
    classifiers=[
        'Programming Language :: Python :: 3',
        'Programming Language :: Python :: 3 :: Only',
        'Programming Language :: Python :: 3.10',
        'Programming Language :: Python :: 3.11',
        'Development Status :: 3 - Alpha',
        'License :: OSI Approved :: GNU Affero General Public License v3'
    ],
    packages=find_packages(exclude=['*.tests']),
    install_requires=[
        'requests >= 2.31, < 3',
        'six >= 1.16, < 2',
        'pandas >= 1.5, < 2',
        'numpy >= 1.23.5, < 2',
        'osmnet >= 0.1.7',
        'pandana >= 0.7, < 0.8',
        'matplotlib >= 3.5, < 3.8',
        'geopy >= 2.0, < 3',
        'pyyaml >= 6.0, < 7',
        'scikit-learn >= 1.2, < 1.4'
    ]
)
