FROM python:3.10-bookworm

RUN pip install psycopg2-binary google-api-python-client isodate paramiko youtube_transcript_api python-dateutil python-dotenv pandas joblib scikit-learn numpy

WORKDIR /usr/src/app

COPY . .

CMD ["sh", "-c", "python codeprod.py && python clusterprod.py && python classificatieprod.py"]