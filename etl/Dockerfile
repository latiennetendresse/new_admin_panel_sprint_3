FROM python:3.9


RUN mkdir /etl

WORKDIR /etl

RUN mkdir storage_data

COPY requirements.txt requirements.txt
COPY entrypoint.sh entrypoint.sh

RUN  pip install --upgrade pip \
    && pip install -r requirements.txt

COPY . /etl

CMD [ "python", "./main.py" ]


