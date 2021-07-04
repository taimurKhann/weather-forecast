FROM python:3.9

WORKDIR /weather_forecast
COPY . .

RUN pip install -r requirements.txt

ENTRYPOINT ["tail", "-f", "/dev/null"]