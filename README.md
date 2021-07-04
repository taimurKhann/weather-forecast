# weather-forecast

### Requirement
* Docker Installed on system

### Libraries
Pandas\
Psycopg2\
Meteostat

### How to run
1) Download the repo on your system or clone it using below command if git is installed\
    **git clone https://github.com/taimurKhann/weather-forecast.git**
2) Move into weather_forecast folder\
    **cd weather_forecast**
3) Run below command to run the docker containers\
    **docker-compose up**
4) Once containers are up and running, open another terminal and run below command\
    **docker exec -it app /bin/bash**
5) Once connected to app conatiner, run below command to execute the job either in historical or daily mode\
    **python main.py historical**\
    or\
    **python main.py daily**
6) After script completed, run below script to get result of average air temperature for the month of February for the 'Berlin / Tegel' Station for all years.\
    **python report.py**
