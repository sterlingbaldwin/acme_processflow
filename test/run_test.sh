echo "activating environment"
source activate workflow

BASE_DIR=/p/cscratch/acme/baldwin32/ci-bot-test
OUTPUT_DIR=$BASE_DIR/output
WORKFLOW_DIR=/export/baldwin32/github/acme_workflow

echo "removing cached ATM file to trigger transfer"
if [ -f $BASE_DIR/input/ATM/case_scripts.cam.h0.0005-12.nc ]; then
    rm $BASE_DIR/input/ATM/case_scripts.cam.h0.0005-12.nc
fi

echo "removing cached output"
if [ -d $OUTPUT_DIR ]; then
    rm -rf $OUTPUT_DIR
fi

echo "starting tests"
python $WORKFLOW_DIR/workflow.py -c $WORKFLOW_DIR/tests/test_run.cfg --no-ui &