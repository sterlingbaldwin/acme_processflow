export VERSION="0.4.54"
export BUILD_NAME="devel"
export CONDA_BLD_PATH=~/conda-bld
USER="acme"
PLATFORM="linux-64"
PKG="processflow"

if [ -d $CONDA_BLD_PATH ]; then
    rm -rf $CONDA_BLD_PATH
fi
echo "Creating build dir at" $CONDA_BLD_PATH
mkdir $CONDA_BLD_PATH

conda config --set anaconda_upload no
if [ ! -z "$1" ]; then
    export TAG="$1"
    echo "Cloning from branch $1" 
else
    export TAG="master"
fi
echo "Building version " $VERSION"-"$BUILD_NAME 
conda build -c acme -c conda-forge -c uvcdat -c lukasz .

if [ ! -z "$1" ]; then
    anaconda upload -u $USER -l "$1" $CONDA_BLD_PATH/$PLATFORM/$PKG-$VERSION-$BUILD_NAME.tar.bz2 
else
    anaconda upload -u $USER $CONDA_BLD_PATH/$PLATFORM/$PKG-$VERSION-$BUILD_NAME.tar.bz2
fi
