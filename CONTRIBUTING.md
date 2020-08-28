## Preparing a release:

- Make a new branch for release prep

- Update the version number and changelog
  - `CHANGELOG.md`
  - `setup.py`
  - `urbanaccess/__init__.py`
  - `docs/source/conf.py`
  - `docs/source/index.rst`

- Make sure all the tests are passing, and check if updates are needed to `README.md` or to the documentation

- Open a pull request to the master branch to finalize it

- After merging, tag the release on GitHub and follow the distribution procedures below


## Distributing a release on PyPI (for pip installation):

- Register an account at https://pypi.org, ask one of the current maintainers to add you to the project, and `pip install twine`

- Check out the copy of the code you'd like to release

- Run `python setup.py sdist bdist_wheel --universal`

- This should create a `dist` directory containing two package files -- delete any old ones before the next step

- Run `twine upload dist/*` -- this will prompt you for your pypi.org credentials

- Check https://pypi.org/project/osmnet/ for the new version


## Distributing a release on Conda Forge (for conda installation):

- The [conda-forge/urbanaccess-feedstock](https://github.com/conda-forge/urbanaccess-feedstock) repository controls the Conda Forge release

- Conda Forge bots usually detect new releases on PyPI and set in motion the appropriate feedstock updates, which a current maintainer will need to approve and merge

- Check https://anaconda.org/conda-forge/urbanaccess for the new version (may take a few minutes for it to appear)