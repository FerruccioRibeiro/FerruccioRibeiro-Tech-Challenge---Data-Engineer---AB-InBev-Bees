#Airflow image
FROM apache/airflow:2.8.1

#Copy requirements
COPY requirements.txt /requirements.txt

#Use airflow user
USER airflow

#Install libs
RUN pip install --no-cache-dir --upgrade pip \
    && pip install --no-cache-dir --user -r /requirements.txt
