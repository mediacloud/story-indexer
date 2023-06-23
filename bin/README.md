This directory is for directly invokable executables (all are likely
to be scripts, but "bin" is shorter, and gets the idea across).

Shell scripts are written using /bin/sh (POSIX standard shell), please
do not assume bash extensions are present!

| file/pattern | usage                                                       |
|--------------|-------------------------------------------------------------|
| func.sh      | shell functions for sourcing (non-excutable)                |
| indirect.sh  | top level script for Docker images invokes run-*.sh scripts |
| run-*.sh     | shell scripts to (preferred way) invoke python programs     |
