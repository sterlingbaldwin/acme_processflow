echo "activating environment"
source activate /p/cscratch/acme/bin/acme

echo "removing cached ATM file to trigger transfer"
if [ -f /p/cscratch/acme/acme_test_user/ci-bot-test/input/ATM/case_scripts.cam.h0.0005-12.nc ]; then
    rm /p/cscratch/acme/acme_test_user/ci-bot-test/input/ATM/case_scripts.cam.h0.0005-12.nc
fi

echo "starting tests"
python /home/acme_test_user/github/acme_workflow/workflow.py -c /home/acme_test_user/github/acme_workflow/tests/test_run.cfg --no-ui &