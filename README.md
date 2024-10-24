# story-indexer

Media Cloud online news story indexer.

## Development

### Setup

To create an environment for development: run `make install` from the
command line.  This creates a virtual environment (venv), installs all
dependencies, and installs a git pre-commit hook.

### "linting"

To run all pre-commit hooks, run `make lint` from the command line.

## Other useful targets

Run `make` (or `make help`) to see the list of other useful targets

### Updating requirements

Required Python libraries are specified in `pyproject.toml`.
`requirements.txt` and `requirements-dev.txt` can be regenerated using
`make upgrade`

## More documentation

To deploy development, staging or production code using Docker Swarm,
see [docker/README.md](docker/README.md)

See also [doc](doc) directory.
