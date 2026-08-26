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
    long_description_content_type='text/x-rst',
    author='UrbanSim Inc. and Samuel D. Blanchard',
    url='https://github.com/UDST/urbanaccess',
    python_requires='>=3.10',
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
        'requests >= 2.32',
        'six >= 1.16',
        'pandas >= 1.5, < 2',  # Pandas 2 compatibility tracked in #95
        'numpy >= 1.26, < 2',  # NumPy 2 requires Pandas 2
        'osmnet >= 0.1.7',
        'pandana >= 0.7',
        'matplotlib >= 3.7',
        'geopy >= 2.4',
        'pyyaml >= 6.0',
        'scikit-learn >= 1.3'
    ]
)
