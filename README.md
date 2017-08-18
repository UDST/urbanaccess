# UrbanAccess
A tool for computing GTFS transit and OSM pedestrian networks for accessibility analysis.

## Related UDST libraries
- [Pandana](https://github.com/UDST/pandana)
- [OSMnet](https://github.com/UDST/osmnet)

# Contributing
Build the Docker image by running `docker build -t udst-urbanaccess .`. Run tests by executing `docker run --rm -v $(pwd):/code udst-urbanaccess ./test.sh`.

When working locally, you may need to clear out the `.pyc` files when running tests after creating container. Do so via: `find . -name \*.pyc -delete`.