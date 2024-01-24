# story-indexer

Media Cloud online news story indexer.

## Development

### Setup

To create an environment for testing from the command line:
create and activate a `python3.10` virtual env and then from the root of the
project, run `make` to install all required dependencies.

```sh
python3.10 -m venv venv
source venv/bin/activate
make
```

### Updating requirements

Required Python libraries are specified in `pyproject.toml`.
`requirements.txt` and `requirements-dev.txt` can be regenerated using
`make upgrade`


### Other useful targets

Run `make help` to see the list of other useful targets

## More documentation

To deploy development, staging or production code using Docker Swarm,
see [docker/README.md](docker/README.md)

See also [doc](doc) directory.
