from time import sleep

if __name__ == "__main__":
    year_count = 1
    month_count = 1
    while True:
        filename = "20161117.beta0.A_WCYCL1850S.ne30_oEC_ICG.edison.cam.h0."
        if year_count < 10:
            filename += "000" + str(year_count)
        elif year_count >= 10 and year_count < 100:
            filename += "00" + str(year_count)

        if month_count < 10:
            filename += "-0" + str(month_count) + ".nc"
        else:
            filename += "-" + str(month_count) + ".nc"

        with open(filename, 'w') as outfile:
            outfile.write("year: {year}, month: {mon}\n".format(
                year=year_count,
                mon=month_count))

        month_count += 1
        if month_count == 13:
            month_count = 1
            year_count += 1
        sleep(10)
