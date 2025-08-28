import os
import time
import google.auth
from dotenv import load_dotenv
from google.oauth2.credentials import Credentials
from google_auth_oauthlib.flow import InstalledAppFlow
from googleapiclient.discovery import build
from googleapiclient.errors import HttpError
from pymongo import MongoClient
from datetime import datetime
from pytube import YouTube, exceptions as pytube_exceptions
# from supabase import create_client, Client, exceptions as supabase_exceptions
from supabase import create_client, Client
from supabase.lib.client_options import ClientOptions as supabase_exceptions
from youtube_transcript_api import YouTubeTranscriptApi, TranscriptsDisabled, NoTranscriptFound, CouldNotRetrieveTranscript

# YouTube API scope
YOUTUBE_SCOPES = ['https://www.googleapis.com/auth/youtube.readonly']

# Environment Variables for Configuration
# NOTE: DO NOT hardcode these values in a public repository
MONGO_URI = os.getenv("MONGO_URI")
SUPABASE_URL = os.getenv("SUPABASE_URL")
SUPABASE_KEY = os.getenv("SUPABASE_KEY")

class VideoProcessor:
    def __init__(self):
        self.youtube_service = self._get_youtube_service()
        self.mongo_client = self._setup_mongo_client()
        self.supabase_client = self._setup_supabase_client()
        self.mongo_collection = self.mongo_client.get_database("youtube_liked_videos").get_collection("videos")
        self.supabase_table = "youtube_videos"

    def _get_youtube_service(self):
        creds = None
        token_path = 'token.json'
        
        if os.path.exists(token_path):
            creds = Credentials.from_authorized_user_file(token_path, YOUTUBE_SCOPES)

        if not creds or not creds.valid:
            if creds and creds.expired and creds.refresh_token:
                creds.refresh(google.auth.transport.requests.Request())
            else:
                flow = InstalledAppFlow.from_client_secrets_file('credentials.json', YOUTUBE_SCOPES)
                creds = flow.run_local_server(port=0)
            
            with open(token_path, 'w') as token:
                token.write(creds.to_json())
        
        return build('youtube', 'v3', credentials=creds)

    def _setup_mongo_client(self):
        if not MONGO_URI:
            raise ValueError("MONGO_URI environment variable not set.")
        return MongoClient(MONGO_URI)

    def _setup_supabase_client(self):
        if not SUPABASE_URL or not SUPABASE_KEY:
            raise ValueError("Supabase URL or Key environment variables not set.")
        return create_client(SUPABASE_URL, SUPABASE_KEY)

    def _get_video_id_from_url(self, url):
        try:
            return url.split('v=')[-1].split('&')[0]
        except IndexError:
            return None

    def _get_transcript(self, youtube_url):
        video_id = self._get_video_id_from_url(youtube_url)
        if not video_id:
            return "No transcript available", None

        # 1. youtube-transcript-api (preferred for robustness)
        try:
            transcript_list = YouTubeTranscriptApi.get_transcript(video_id, languages=['en', 'ta'])
            transcript = " ".join([entry['text'] for entry in transcript_list])
            return transcript, 'yta'
        except (TranscriptsDisabled, NoTranscriptFound, CouldNotRetrieveTranscript):
            pass  # Fallback to pytube
        except Exception as e:
            print(f"Error with youtube-transcript-api: {e}")

        # 2. pytube (fallback)
        try:
            yt = YouTube(youtube_url)
            caption = yt.captions.get_by_language_code('en') or yt.captions.get_by_language_code('ta')
            if caption:
                return caption.generate_srt_captions(), 'pytube'
        except pytube_exceptions.PytubeError as e:
            print(f"Error with pytube: {e}")
        except Exception as e:
            print(f"Unexpected error with pytube: {e}")

        return "No transcript available", None

    def _check_if_video_exists(self, video_id):
        exists_mongo = self.mongo_collection.find_one({'video_id': video_id})
        
        try:
            exists_supabase_data = self.supabase_client.table(self.supabase_table).select("video_id").eq("video_id", video_id).execute().data
            exists_supabase = len(exists_supabase_data) > 0
        except supabase_exceptions.APIError as e:
            print(f"Supabase API error: {e}")
            exists_supabase = False
        
        return exists_mongo or exists_supabase

    def _insert_video_data(self, video_data):
        try:
            # MongoDB insertions
            self.mongo_collection.insert_one(video_data)
            
            # Supabase insertion
            supabase_data = dict(video_data)
            supabase_data.pop('_id', None)
            self.supabase_client.table(self.supabase_table).insert(supabase_data).execute()
            
            print(f"Successfully inserted video: {video_data['title']}")
        except Exception as e:
            print(f"Error inserting video data for {video_data.get('title')}: {e}")

    def run(self):
        try:
            request = self.youtube_service.videos().list(
                part='snippet',
                myRating='like',
                maxResults=50
            )
            response = request.execute()
            liked_videos = response.get('items', [])
            print(f"Found {len(liked_videos)} liked videos to process.")

            for video_item in liked_videos:
                video_id = video_item['id']
                if self._check_if_video_exists(video_id):
                    print(f"Skipping existing video: {video_id}")
                    continue

                youtube_url = f"https://www.youtube.com/watch?v={video_id}"
                transcript, method_lang = self._get_transcript(youtube_url)
                
                video_data = {
                    'video_id': video_id,
                    'title': video_item['snippet']['title'],
                    'published_at': video_item['snippet']['publishedAt'],
                    'description': video_item['snippet'].get('description', ''),
                    'youtube_url': youtube_url,
                    'added_at': datetime.now().isoformat(),
                    'transcript': transcript
                }
                
                self._insert_video_data(video_data)
                time.sleep(1) # Delay to avoid rate limits

        except HttpError as e:
            print(f"YouTube API error: {e}")
        except Exception as e:
            print(f"An unexpected error occurred: {e}")

if __name__ == '__main__':
    #load_dotenv() # This loads the variables from .env
    processor = VideoProcessor()
    processor.run()