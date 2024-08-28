import spotipy
import os
from spotipy.oauth2 import SpotifyClientCredentials
import logging
import pandas as pd
from dotenv import load_dotenv


def authenticate_spotify(client_id, client_secret):
    try:
        sp = spotipy.Spotify(auth_manager=SpotifyClientCredentials(client_id=client_id, client_secret=client_secret))
        logging.info("Autenticación exitosa con Spotify")
        return sp
    except Exception as e:
        logging.error(f"Error en la autenticación con Spotify: {e}")
        exit(1)

def get_tracks_data(sp, years):
    data = []
    for year in years:
        try:
            results = sp.search(q=f'year:{year}', type='track', limit=50)
            for track in results['tracks']['items']:
                id = track['id']
                artist_name = track['artists'][0]['name']
                artist_id = track['artists'][0]['id']
                track_name = track['name'].replace("'", "")  # Quitar las comillas simples
                duration_ms = track['duration_ms']
                album_name = track['album']['name'].replace("'", "")  # Quitar las comillas simples
                album_img = track['album']['images'][0]['url']  # Imagen del álbum
                album_total_tracks = track['album']['total_tracks']
                track_genres = ', '.join(sp.artist(artist_id)['genres'])  # Separar el género por coma
                track_popularity = track['popularity']
                release_date = track['album']['release_date']

                # Agregar los datos a la lista
                data.append([id, artist_name, track_name, duration_ms, track_genres, album_name, album_img, album_total_tracks, track_popularity, release_date])
            logging.info(f"Datos obtenidos para el año {year}")
        except Exception as e:
            logging.error(f"Error al buscar pistas para el año {year}: {e}")
    return data

def create_dataframe(data):
    df = pd.DataFrame(data, columns=['Id', 'Artista', 'Cancion', 'Duracion_ms', 'Genero', 'Album', 'Album_img', 'Total_canciones_album', 'Popularidad', 'fecha_lanzamiento'])
    
    # Convertir 'duracion_ms' de milisegundos a minutos
    df['Duracion_ms'] = df['Duracion_ms'] / 60000
    # Evitar canciones duplicadas
    df = df.drop_duplicates(subset=['Artista', 'Cancion', 'Album'], keep='first')
    #aca se reemplaza el campo genero vacio por desconocido
    df['Genero'] = df['Genero'].replace('', 'Desconocido').fillna('Desconocido')
    #aca se evitan cacniones con duracion de 0Seg
    df = df[df['Duracion_ms'] != 0]
    # Convertir 'fecha_lanzamiento' a formato de fecha
    df['fecha_lanzamiento'] = pd.to_datetime(df['fecha_lanzamiento'], errors='coerce')
    
    return df
