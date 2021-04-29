Thanks for using UrbanAccess! 

This is an open source project that's part of the Urban Data Science Toolkit. Development and maintenance is a collaboration between UrbanSim Inc, U.C. Berkeley's Urban Analytics Lab, and other contributors.

## If you have a problem:

- Take a look at the [open issues](https://github.com/UDST/urbanaccess/issues) and [closed issues](https://github.com/UDST/urbanaccess/issues?q=is%3Aissue+is%3Aclosed) to see if there's already a related discussion

- Open a new issue describing the problem -- if possible, include any error messages, a full reproducible example of the code that generated the error, the operating system and version of Python you're using, and versions of any libraries that may be relevant

## Feature proposals:

- Take a look at the [open issues](https://github.com/UDST/urbanaccess/issues) and [closed issues](https://github.com/UDST/urbanaccess/issues?q=is%3Aissue+is%3Aclosed) to see if there's already a related discussion

- Post your proposal as a new issue, so we can discuss it (some proposals may not be a good fit for the project)

## Contributing code:

- Create a new branch of `UDST/urbanaccess/dev`, or fork the repository to your own account

- Make your changes, following the existing styles for code and inline documentation

- Add [tests](https://github.com/UDST/urbanaccess/tree/dev/urbanaccess/tests) if possible
  - We use the test suite: Pytest
  
- Run tests and address any issues that may be flagged. If flags are raised that are not due to the PR note that in a new comment in the PR
  - Run Pytest test suite: `py.test`
    - UrbanAccess currently supports Python 2.7, 3.5, 3.6, 3.7, 3.8. Tests will be run in these environments when the PR is created but any flags raised in these environments should also be addressed
  - UrbanAccess also uses a series of integration tests to test entire workflows, run the integration tests:
    - Run:
    ```cd demo
    jupyter nbconvert --to python simple_example.ipynb
    cd ../urbanaccess/tests/integration
    python remove_nb_magic.py -in simple_example.py -out simple_example_clean.py
    cd ../../../demo
    python simple_example_clean.py
    cd ../urbanaccess/tests/integration
    python integration_madison.py
    python integration_sandiego.py
  - Run pycodestyle Python style guide checker: `pycodestyle --max-line-length=100 urbanaccess`

- Open a pull request to the `UDST/urbanaccess` `dev` branch, including a writeup of your changes -- take a look at some of the closed PR's for examples

- Current maintainers will review the code, suggest changes, and hopefully merge it and schedule it for an upcoming release

## Updating the documentation: 

- See instructions in `docs/README.md`

## Preparing a release:

- Make a new branch for release prep

- Update the version number and changelog:
  - `CHANGELOG.md`
  - `setup.py`
  - `urbanaccess/__init__.py`
  - `docs/source/index.rst`
  - `docs/source/conf.py`

- Make sure all the tests are passing, and check if updates are needed to `README.md` or to the documentation

- Open a pull request to the `dev` branch to finalize it and wait for a PR review and approval

- After the PR has been approved, it can be merged to `dev`. Then a release PR can be created from `dev` to merge into `master`. Once merged, tag the release on GitHub and follow the distribution procedures below:

## Distributing a release on PyPI (for pip installation):

- Register an account at https://pypi.org, ask one of the current maintainers to add you to the project, and `pip install twine`

- Check out the copy of the code you'd like to release

- Run `python setup.py sdist bdist_wheel --universal`

- This should create a `dist` directory containing a gzip package file -- delete any old ones before the next step

- Run `twine upload dist/*` -- this will prompt you for your pypi.org credentials

- Check https://pypi.org/project/urbanaccess/ for the new version


## Distributing a release on Conda Forge (for conda installation):

- The [conda-forge/urbanaccess-feedstock](https://github.com/conda-forge/urbanaccess-feedstock) repository controls the Conda Forge release, including which GitHub users have maintainer status for the repo

- Conda Forge bots usually detect new releases on PyPI and set in motion the appropriate feedstock updates, which a current maintainer will need to approve and merge

- Maintainers can add on additional changes before merging the PR, for example to update the requirements or edit the list of maintainers

- You can also fork the feedstock and open a PR manually. It seems like this must be done from a personal account (not a group account like UDST) so that the bots can be granted permission for automated cleanup

- Check https://anaconda.org/conda-forge/urbanaccess for the new version (may take a few minutes for it to appear)