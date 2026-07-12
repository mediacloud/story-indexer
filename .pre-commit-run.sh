#!/bin/sh


# Invoked from .pre-commit-config.yaml to run mypy (or other tool)
# using "pre-commit" variable in pyproject.toml file
# project.optional-dependencies section.

# from rss-fetcher, from mc-providers, from es-tools, from sitemap-tools

# NOTE!! Takes FULL command line as arguments
LOG=$0.log
(
  date
  pwd
  echo COMMAND LINE: $0 $*
  echo '#####'
  echo ENVIRONMENT:
  env
  echo '#####'
) > $LOG

# NOTE!! https://github.com/pre-commit/mirrors-mypy/README.md says
# "using the --install-types is problematic." (mutates cache)

# Want to stash copy of pyproject.toml in the top level of the
# pre-commit created virtual environment to detect changes.
# Fortunately, a useful variable points there!
if [ -z "$VIRTUAL_ENV" ]; then
    echo "$0: VIRTUAL_ENV not set; see $LOG" 1>&2
    exit 1
fi

# check if package lists have changed, and re-install if needed:
check_install() {
    FN=$1
    shift
    # remaining args are passed to package installer

    TMP=$VIRTUAL_ENV/.$FN
    echo TMP $TMP >> $LOG
    if cmp -s $FN $TMP; then
	echo no change to $FN >> $LOG
    else
	echo $FN changed: pip install $* >> $LOG
	if python3 -m pip install $*; then
	    cp -p $FN $TMP
	else
	    STATUS=$?
	    echo pip install $* failed $STATUS >> $LOG
	    exit $STATUS
	fi
    fi
}

# NOTE! using pip-tools generated requirements-pre.txt
check_install requirements-pre.txt -r requirements-pre.txt

#pip list >> $LOG
# NOTE! first arg must be command to invoke!
"$@"
