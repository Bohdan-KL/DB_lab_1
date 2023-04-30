FROM python:3.11-slim-buster

WORKDIR /DB_Lab_1/app

RUN pip install --upgrade pip
COPY requirements.txt /DB_Lab_1/app/requirements.txt
RUN pip install -r requirements.txt

COPY . /DB_Lab_1/app

CMD ["python", "main.py"]
