FROM python:3.10

WORKDIR /etl

RUN mkdir storage_data

COPY requirements.txt requirements.txt

RUN  pip install --upgrade pip \
    && pip install -r requirements.txt

COPY . /etl

CMD [ "python", "./main.py" ]


