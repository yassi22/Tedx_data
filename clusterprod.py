import os
import pickle
import psycopg2
from dotenv import load_dotenv
import pandas as pd
import logging

# Zet logging aan
logging.basicConfig(level=logging.INFO)

# Laad omgevingsvariabelen
load_dotenv()

# Model en scaler laden
def load_model_and_scaler(model_path, scaler_path):
    with open(model_path, 'rb') as model_file:
        model = pickle.load(model_file)
    with open(scaler_path, 'rb') as scaler_file:
        scaler = pickle.load(scaler_file)
    return model, scaler

# Verbinding maken met database
def connect_to_db():
    try:
        conn = psycopg2.connect(
            dbname=os.getenv('DB_NAME'),
            user=os.getenv('DB_USER'),
            password=os.getenv('DB_PASSWORD'),
            host=os.getenv('DB_HOST'),
            port=os.getenv('DB_PORT')
        )
        return conn
    except Exception as e:
        logging.error(f"Fout bij verbinden met de database: {str(e)}")
        raise

# Haal video-IDs op vanuit een Docker volume map
def fetch_video_ids_from_volume(local_path):
    try:
        logging.info(f"Ophalen van video-IDs uit map: {local_path}")
        video_ids = os.listdir(local_path)
        return [vid for vid in video_ids if not vid.startswith('.')]  # Negeer verborgen bestanden
    except Exception as e:
        logging.error(f"Fout bij ophalen van video-ID's: {str(e)}")
        return []

# Haal relevante videodata op uit de database
def fetch_video_data(cur, video_id):
    try:
        cur.execute("""
            SELECT 
                vd.videoid, vd.title, sf.view_count, sf.like_count, sf.comment_count, vd.duration
            FROM 
                statistics_fact sf
            JOIN 
                video_dimension vd ON sf.videoid = vd.videoid
            WHERE 
                sf.videoid = %s
            ORDER BY 
                sf.retrieved_date DESC;
        """, (video_id,))
        return cur.fetchone()
    except Exception as e:
        logging.error(f"Fout bij ophalen van data voor video {video_id}: {str(e)}")
        return None

def classify_video(model, scaler, video_data):
    try:
        # Unpack video data: (video_id, title, views, likes, comment_count, duration)
        video_id, title, views, likes, comment_count, duration = video_data

        # Maak DataFrame met de juiste kolommen (4 features)
        df = pd.DataFrame([[views, likes, comment_count, duration]],
                          columns=['views', 'likes', 'comment_count', 'duration'])

        # Schaal de data
        df_scaled = pd.DataFrame(scaler.transform(df), columns=df.columns)

        # Voorspel of de video populair is
        prediction = model.predict(df_scaled)

        # Return video_id, title, and popularity rating
        return video_id, title, 'populair' if prediction[0] == 0 else 'unpopulair'
    except Exception as e:
        logging.error(f"Error while classifying the video: {str(e)}")
        return None

# Voeg kolom toe indien niet aanwezig
def add_popularity_column_if_not_exists(cur):
    try:
        cur.execute("""
            DO $$
            BEGIN
                IF NOT EXISTS (
                    SELECT 1 
                    FROM information_schema.columns 
                    WHERE table_name='video_dimension' 
                    AND column_name='popularity_rating'
                ) THEN
                    ALTER TABLE video_dimension ADD COLUMN popularity_rating VARCHAR(50);
                END IF;
            END $$;
        """)
    except Exception as e:
        logging.error(f"Fout bij het toevoegen van de kolom: {str(e)}")

def insert_popular_video(cur, video_id, title, popularity_rating):
    try:
        add_popularity_column_if_not_exists(cur)
        cur.execute("""
            INSERT INTO video_dimension (videoid, title, popularity_rating)
            VALUES (%s, %s, %s)
            ON CONFLICT (videoid) DO UPDATE SET popularity_rating = EXCLUDED.popularity_rating;
        """, (video_id, title, popularity_rating))
    except Exception as e:
        logging.error(f"Fout bij het opslaan van de video {video_id}: {str(e)}")

# Hoofdprogramma
def main():
    # Verbinding maken met de database
    conn = connect_to_db()
    cur = conn.cursor()

    # Laad het model en de scaler
    model, scaler = load_model_and_scaler('kmeans_model_V1_5.pkl', 'scaler_V1_5.pkl')

    # Haal video-IDs op vanuit het Docker volume
    local_path = "/video-container"
    video_ids = fetch_video_ids_from_volume(local_path)
    logging.info(f"Gevonden video-IDs: {video_ids[:5]}{'...' if len(video_ids) > 5 else ''}")

    # Classificeer video's en sla ze op
    for video_id in video_ids:
        video_data = fetch_video_data(cur, video_id)
        if video_data:
            result = classify_video(model, scaler, video_data)
            if result:
                video_id, title, popularity_rating = result
                insert_popular_video(cur, video_id, title, popularity_rating)

    # Commit wijzigingen en sluit de verbinding
    conn.commit()
    cur.close()
    conn.close()
    logging.info("Populaire video's zijn opgeslagen in de database.")

if __name__ == "__main__":
    main()
