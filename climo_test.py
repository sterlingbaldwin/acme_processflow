from jobs.Ncclimo import Climo
import os

if __name__ == "__main__":
    climo = Climo({
        'start_year': "0001",
        'end_year': "0002",
        'caseId': "20161117.beta0.A_WCYCL1850S.ne30_oEC_ICG.edison",
        'annual_mode': 'sdd',
        'regrid_map_path': os.getcwd() + '/resources/map_ne30np4_to_fv129x256_aave.20150901.nc',
        'input_directory': "/space2/sbaldwin/",
        'climo_output_directory': '/space2/sbaldwin/output/',
        'regrid_output_directory': '/space2/sbaldwin/regrid/',
        'yearset': 0
    })
    print "starting ncclimo"
    climo.execute()
    print "ncclimo complete"
    