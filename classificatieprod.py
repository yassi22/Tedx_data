import pandas as pd
import joblib
import psycopg2
import os
from dotenv import load_dotenv

load_dotenv()


database_name = os.getenv('DB_NAME')
database_user = os.getenv('DB_USER')
database_password = os.getenv('DB_PASSWORD')
database_host = os.getenv('DB_HOST')
database_port = os.getenv('DB_PORT')

# Verbinding maken met de lokale PostgreSQL-database
conn = psycopg2.connect(
        database=database_name,
        user=database_user,
        password=database_password,
        host=database_host,
        port=database_port
    )

# Query om video- en transcriptgegevens op te halen
query = """
    SELECT video_transcript_dimension.video_id, video_transcript_dimension.transcript_id, video_transcript_dimension.text
    FROM video_transcript_dimension
"""
transcripts_df = pd.read_sql(query, conn)

# Sluit de lokale databaseverbinding
conn.close()

# Laad de opgeslagen CountVectorizer en classifier
count_vectorizer = joblib.load('nlp_model.pkl')
clf = joblib.load('classificatie_model.pkl')

# Neem aan dat de transcripts in een kolom genaamd 'text' staan
transcripts = transcripts_df['text']

# Transformeer de transcripts met de geladen CountVectorizer
transcripts_bow = count_vectorizer.transform(transcripts)

# Voorspel de sentimenten met de geladen classifier
predictions = clf.predict(transcripts_bow.toarray())

# Koppel voorspellingen aan 'positief' of 'negatief'
transcripts_df['predicted_sentiment'] = ['positive' if pred == 1 else 'negative' for pred in predictions]

# Verbinding maken met de lokale PostgreSQL-database om de sentimentgegevens op te slaan
conn = psycopg2.connect(
        database=database_name,
        user=database_user,
        password=database_password,
        host=database_host,
        port=database_port
    )
# Maak een cursor object
cur = conn.cursor()

# Functie om de sentimentkolom toe te voegen als deze nog niet bestaat
def add_sentiment_column_if_not_exists(cur):
    try:
        cur.execute("""
            DO $$
            BEGIN
                IF NOT EXISTS (
                    SELECT 1 
                    FROM information_schema.columns 
                    WHERE table_name='video_dimension' 
                    AND column_name='sentiment'
                ) THEN
                    ALTER TABLE video_dimension ADD COLUMN sentiment VARCHAR(50);
                END IF;
            END $$;
        """)
        print("Kolom 'sentiment' is toegevoegd aan de tabel 'video_dimension' (indien deze nog niet bestond).")
    except Exception as e:
        print(f"Fout bij het toevoegen van de kolom: {str(e)}")

# Voeg de sentimentkolom toe als deze nog niet bestaat
add_sentiment_column_if_not_exists(cur)

# Update de sentimentgegevens in de video_dimension tabel
for index, row in transcripts_df.iterrows():
    cur.execute("""
        UPDATE video_dimension
        SET sentiment = %s
        WHERE videoid = %s;
    """, (row['predicted_sentiment'], row['video_id']))

# Bevestig de wijzigingen en sluit de verbinding
conn.commit()
cur.close()
conn.close()

# Print de resultaten
print(transcripts_df)