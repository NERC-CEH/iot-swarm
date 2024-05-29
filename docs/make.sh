SPHINXBUILD=sphinx-build
SOURCEDIR=.
BUILDDIR=_build
SPHINXOPTS=""



function help() {
    $SPHINXBUILD -M help $SOURCEDIR $BUILDDIR $SPHINXOPTS
}

function apidoc() {
    args=$@
    if [ -z "$args" ]; then
        args="--module-first --force --private"
    fi
    
    sphinx-apidoc $(echo $args) -o source ../src/iotdevicesimulator
}

function build() {
    args=$@
    if [ -z "$args" ]; then
        args="-M html"
    fi

    $SPHINXBUILD $(echo $args) $SOURCEDIR $BUILDDIR $SPHINXOPTS -a
}

if [ "$1" == "help" ]; then
    help
elif [ "$1" == "apidoc" ]; then
    shift
    apidoc $@
elif [ "$1" == "build" ]; then
    shift
    build $@
fi