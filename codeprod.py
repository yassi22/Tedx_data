from datetime import datetime

import googleapiclient.discovery
import googleapiclient.errors
import isodate
import psycopg2
import time
import youtube_transcript_api
from cryptography.hazmat.primitives.serialization import ssh
from youtube_transcript_api import YouTubeTranscriptApi
from datetime import datetime, timedelta
from dateutil import parser
from dotenv import load_dotenv
import os



def create_dimension_tables(cur, conn):
    # Time dimension
    cur.execute("""
       CREATE TABLE IF NOT EXISTS Time_Dimension (
           datum DATE PRIMARY KEY
       );
       """)

    # Channel dimension
    cur.execute("""
    CREATE TABLE IF NOT EXISTS Channel_Dimension (
        channelid VARCHAR(255) PRIMARY KEY,
        name VARCHAR(255),
        subscribers BIGINT
    );
    """)

    # Video dimension
    cur.execute("""
    CREATE TABLE IF NOT EXISTS Video_Dimension (
        videoid VARCHAR(255) PRIMARY KEY,
        title VARCHAR(255),
        description TEXT,
        url TEXT,
        published_date TIMESTAMP,
        duration INT,
        channelid VARCHAR(255),
        FOREIGN KEY (channelid) REFERENCES Channel_Dimension (channelid)
    );
    """)

    # Video Transcript dimension
    cur.execute("""
    CREATE TABLE IF NOT EXISTS Video_Transcript_Dimension (
        transcript_id SERIAL UNIQUE PRIMARY KEY,
        video_id VARCHAR(255),
        text TEXT,
        start_time FLOAT,
        duration FLOAT,
        FOREIGN KEY (video_id) REFERENCES Video_Dimension (videoid)
    );
    """)

    # Fact table
    cur.execute("""
        CREATE TABLE IF NOT EXISTS Statistics_Fact (
            statsid SERIAL PRIMARY KEY,
            channelid VARCHAR(255),
            batchid INT,
            videoid VARCHAR(255),
            published_date DATE,
            view_count BIGINT,
            like_count BIGINT,
            comment_count BIGINT,
            retrieved_date TIMESTAMP,  -- Add retrieved_date column
            FOREIGN KEY (channelid) REFERENCES Channel_Dimension (channelid),
            FOREIGN KEY (videoid) REFERENCES Video_Dimension (videoid),
            FOREIGN KEY (published_date) REFERENCES Time_Dimension (datum)
        );
    """)

    conn.commit()

# Functie om datums te genereren voor een volledig jaar
def generate_year_dates(start_year, end_year):
    dates = []
    for year in range(start_year, end_year + 1):
        start_date = datetime(year, 1, 1)
        days_in_year = 366 if is_leap_year(year) else 365
        dates += [start_date + timedelta(days=i) for i in range(days_in_year)]
    return dates

# Functie om te controleren of een jaar een schrikkeljaar is
def is_leap_year(year):
    return (year % 4 == 0 and year % 100 != 0) or (year % 400 == 0)


# Invoegen van een rij in de Time_Dimension tabel, waarbij dubbele invoer wordt voorkomen
def insert_time_dimension(cur, conn, date):
    cur.execute("""
        INSERT INTO Time_Dimension (datum)
        VALUES (%s)
        ON CONFLICT (datum) DO NOTHING
    """, (date,))
    conn.commit()
    return date  # Alleen de datum teruggeven


# Invoegen van alle datums voor een reeks jaren in de Time_Dimension tabel

def insert_years_into_time_dimension(cur, conn, start_year, end_year):
    dates = generate_year_dates(start_year, end_year)
    for date in dates:
        insert_time_dimension(cur, conn, date)



def insert_channel_dimension(cur, conn, channel_id, name, subscribers):
    cur.execute("""
    INSERT INTO Channel_Dimension (channelid, name, subscribers)
    VALUES (%s, %s, %s)
    ON CONFLICT (channelid) DO UPDATE 
    SET name = EXCLUDED.name, subscribers = EXCLUDED.subscribers;
    """, (channel_id, name, subscribers))
    conn.commit()


def insert_video_dimension(cur, conn, video_id, title, description, url, published_date, duration, channel_id):
    cur.execute("""
    INSERT INTO Video_Dimension (videoid, title, description, url, published_date, duration, channelid)
    VALUES (%s, %s, %s, %s, %s, %s, %s)
    ON CONFLICT (videoid) DO UPDATE 
    SET title = EXCLUDED.title, description = EXCLUDED.description, 
        url = EXCLUDED.url, published_date = EXCLUDED.published_date, 
        duration = EXCLUDED.duration, channelid = EXCLUDED.channelid;
    """, (video_id, title, description, url, published_date, duration, channel_id))
    conn.commit()


def insert_video_transcript_dimension(cur, conn, video_id, text, start_time, duration):
    # Controleer of de transcript al bestaat
    cur.execute("""
        SELECT EXISTS (
            SELECT 1 FROM Video_Transcript_Dimension 
            WHERE video_id = %s AND start_time = %s AND duration = %s
        );
    """, (video_id, start_time, duration))

    exists = cur.fetchone()[0]

    if not exists:
        cur.execute("""
            INSERT INTO Video_Transcript_Dimension (video_id, text, start_time, duration)
            VALUES (%s, %s, %s, %s)
            RETURNING transcript_id;
        """, (video_id, text, start_time, duration))
        conn.commit()
        result = cur.fetchone()
        return result[0] if result else None
    else:
        return None


def insert_statistics_fact(cur, conn, channel_id, batch_id, video_id, datum, view_count,
                           like_count, comment_count, retrieved_date):
    cur.execute("""
        INSERT INTO Statistics_Fact (channelid, batchid, videoid, published_date, view_count, 
                                     like_count, comment_count, retrieved_date)
        VALUES (%s, %s, %s, %s, %s, %s, %s, %s)
    """, (
        channel_id, batch_id, video_id, datum, view_count, like_count, comment_count, retrieved_date))




# Main function to process video data
def process_video_data(cur, conn, video_id, channel_id, title, description, url, published_date, duration, view_count,
                       like_count, comment_count, transcript):
    # Insert into dimension tables
    insert_channel_dimension(cur, conn, channel_id, "Channel Name", 0)  # Fetch actual channel name and subscribers
    insert_video_dimension(cur, conn, video_id, title, description, url, published_date, duration, channel_id)

    # Process transcript
    transcript_id = None
    if transcript:
        full_text = ' '.join([entry['text'] for entry in transcript])
        start_time = transcript[0]['start']
        transcript_duration = transcript[-1].get('duration', 0) + transcript[-1]['start'] - start_time

        # Insert transcript and return the ID
        transcript_id = insert_video_transcript_dimension(cur, conn, video_id, full_text, start_time, transcript_duration)

    # Insert time dimension and get calender_id
    datum = insert_time_dimension(cur, conn, published_date.date())  # Zorg ervoor dat je de datum zonder tijd gebruikt

    # Insert into fact table
    batch_id = int(time.time())  # Using current timestamp as batch_id

    # Pass the transcript_id to the fact table insertion, even if it's None (if no transcript)
    insert_statistics_fact(cur, conn, channel_id, batch_id, video_id, datum, view_count,
                           like_count, comment_count)



def fetch_video_ids_from_volume(volume_path):
    """Fetch video IDs from a mounted Docker volume."""
    try:
        # List the contents of the mounted volume
        print(f"Listing contents of directory: {volume_path}")
        directory_contents = os.listdir(volume_path)

        # Filter out hidden files and directories
        video_ids = [item for item in directory_contents if not item.startswith('.')]
        print(f"Found {len(video_ids)} potential video IDs.")

        # Return the list of video IDs
        return video_ids

    except OSError as e:
        print(f"Error accessing the Docker volume: {str(e)}")
        return []

if __name__ == "__main__":
    server_ip = os.getenv('SERVER_IP')
    username = os.getenv('SERVER_USER')
    password = os.getenv('SERVER_PASSWORD')
    directory_path = "/video-container"


    video_ids = fetch_video_ids_from_volume(directory_path)

    if video_ids:
        print("Opgehaalde video-ID's:")
        for vid in video_ids[:10]:  # Print first 10 as an example
            print(vid)
        print(f"... and {len(video_ids) - 10} more.")
    else:
        print("Geen video-ID's gevonden.")



# Fetch video transcript and insert into database
def fetch_transcript(video_id):
    try:
        transcript = YouTubeTranscriptApi.get_transcript(video_id)
        return transcript
    except youtube_transcript_api._errors.NoTranscriptFound as e:
        print(f"No transcript found for video {video_id}: {str(e)}")
        return None
    except Exception as e:
        print(f"Could not fetch transcript for video {video_id}: {str(e)}")
        return None


def insert_transcript_to_db(cur, conn, video_id, transcript):
    full_text = ' '.join([entry['text'] for entry in transcript])
    sql = """
    INSERT INTO Video_Transcript_Dimension (video_id, text, start_time, duration)
    VALUES (%s, %s, %s, %s);
    """
    start_time = transcript[0]['start']
    duration = transcript[-1].get('duration', 0) + transcript[-1]['start'] - start_time
    cur.execute(sql, (video_id, full_text, start_time, duration))
    conn.commit()


# Main function
def main():
    # Database connection

    load_dotenv()

    print("Omgevingsvariabelen laden...")
    db_user = os.getenv('DB_USER')
    db_password = os.getenv('DB_PASSWORD')


    database_name =  os.getenv('DB_NAME')
    database_user = os.getenv('DB_USER')
    database_password = os.getenv('DB_PASSWORD')
    database_host = os.getenv('DB_HOST')
    database_port = os.getenv('DB_PORT')

    # Verbinding maken met de database
    conn = psycopg2.connect(
        dbname=database_name,
        user=database_user,
        password=database_password,
        host='95.217.3.61',
        port=database_port
    )
    cur = conn.cursor()

    # Create tables if they don't exist
    create_dimension_tables(cur,conn)

    insert_years_into_time_dimension(cur, conn, 2022, 2024)

    # YouTube API setup
    api_service_name = "youtube"
    api_version = "v3"
    api_key = os.getenv('YOUTUBE_API_KEY')  # Replace with your own API key
    youtube = googleapiclient.discovery.build(api_service_name, api_version, developerKey=api_key)

    # Fetch video IDs from the server
    video_ids = fetch_video_ids_from_volume(directory_path)

    if not video_ids:
        print("No video IDs found.")
        return

    # TED channel ID for testing
    channel_id = "UCAuUUnT6oDeKwE6v1NGQxug"

    # Fetch channel information
    channel_info = youtube.channels().list(part="snippet,statistics", id=channel_id).execute()
    channel_name = channel_info["items"][0]["snippet"]["title"]
    channel_subscribers = channel_info["items"][0]["statistics"]["subscriberCount"]

    # Insert channel data into the database
    insert_channel_dimension(cur, conn, channel_id, channel_name, channel_subscribers)

    # Fetch and process each video by ID
    # Verwerk elke video op basis van ID
    for video_id in video_ids:
        # Haal videogegevens en statistieken op
        video_request = youtube.videos().list(part="snippet,statistics,contentDetails", id=video_id).execute()
        video_data = video_request['items'][0]

        # Videogegevens
        video_title = video_data["snippet"]["title"]
        description = video_data["snippet"]["description"]
        url = f"https://www.youtube.com/watch?v={video_id}"
        published_date = parser.isoparse(video_data["snippet"]["publishedAt"]).date()

        # Verkrijg de datum zonder tijd
        iso_duration = video_data["contentDetails"]["duration"]
        duration = isodate.parse_duration(iso_duration).total_seconds()

        # Video statistieken
        stats = video_data["statistics"]
        view_count = stats.get("viewCount", 0)
        like_count = stats.get("likeCount", 0)
        comment_count = stats.get("commentCount", 0)

        # Voer de invoegoperaties uit
        insert_video_dimension(cur, conn, video_id, video_title, description, url, published_date, duration, channel_id)

        # Insert time dimension and get datum
        datum = insert_time_dimension(cur, conn, published_date)  # Zorg ervoor dat je de datum doorgeeft

        # Fetch en insert transcript in de database
        transcript = fetch_transcript(video_id)
        transcript_id = None
        if transcript:
            transcript_id = insert_video_transcript_dimension(cur, conn, video_id,
                                                              ' '.join([entry['text'] for entry in transcript]),
                                                              transcript[0]['start'], transcript[-1]['duration'])

        # Insert statistics fact AFTER transcript is processed
        batch_id = int(time.time())  # Gebruik huidige tijd als batch ID
        retrieved_date = datetime.now()
        insert_statistics_fact(cur, conn, channel_id, batch_id, video_id, datum, view_count,
                               like_count, comment_count, retrieved_date)

    print("Videos, stats, and transcripts have been processed and stored in the database.")

    # Close the database connection
    cur.close()
    conn.close()


if __name__ == "__main__":
    main()