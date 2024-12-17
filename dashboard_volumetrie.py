import pymysql
import psycopg2
import sqlite3
import cx_Oracle
import pyodbc
import pandas as pd
import streamlit as st
from PIL import Image  # Pour afficher le logo

# Fonction pour se connecter à la base de données en fonction du type
def connect_to_db(db_type, host, user, password, database):
    try:
        if db_type == 'MySQL':
            connection = pymysql.connect(host=host, user=user, password=password, database=database)
        elif db_type == 'PostgreSQL':
            connection = psycopg2.connect(host=host, user=user, password=password, dbname=database)
        elif db_type == 'SQLite':
            connection = sqlite3.connect(database)
        elif db_type == 'Oracle':
            dsn_tns = cx_Oracle.makedsn(host, 1521, service_name=database)
            connection = cx_Oracle.connect(user=user, password=password, dsn=dsn_tns)
        elif db_type == 'SQL Server':
            connection = pyodbc.connect(f'DRIVER={{SQL Server}};SERVER={host};DATABASE={database};UID={user};PWD={password}')
        else:
            raise ValueError("Type de base de données non pris en charge")
        return connection
    except Exception as e:
        st.error(f"Erreur de connexion à la base de données: {str(e)}")
        return None

# Fonction pour traiter un fichier CSV ou Excel en morceaux
def process_large_file(uploaded_file, chunk_size=100000):
    try:
        # Lire le fichier en morceaux
        if uploaded_file.name.endswith('.csv'):
            chunks = pd.read_csv(uploaded_file, chunksize=chunk_size)
        elif uploaded_file.name.endswith('.xlsx'):
            chunks = pd.read_excel(uploaded_file, chunksize=chunk_size)
        else:
            st.error("Type de fichier non supporté. Veuillez télécharger un fichier CSV ou Excel.")
            return None
        
        # Afficher les morceaux et leur taille
        for chunk in chunks:
            total_size = chunk.memory_usage(deep=True).sum() / 1024 / 1024  # Taille en Mo
            st.write(f"Taille du morceau: {total_size:.2f} MB")
            st.dataframe(chunk.head())  # Afficher un aperçu du morceau
        
        return chunks  # Retourner les morceaux traités

    except Exception as e:
        st.error(f"Erreur lors du traitement du fichier: {str(e)}")
        return None

# Si vous voulez définir une taille spécifique pour l'image (par exemple 200px de largeur)
st.sidebar.image("assets/logo_orange.png", width=200)

# Interface utilisateur pour saisir les informations de la base de données
st.title("Dashboard de Volumétrie des Bases de Données")

# Choisir la source de données
data_source = st.radio("Choisissez la source des données", ('Base de données', 'Fichier'))

if data_source == 'Base de données':
    # Entrée des informations de connexion
    st.subheader("Connexion à la Base de Données")
    db_type = st.selectbox("Choisissez le type de base de données", ['MySQL', 'PostgreSQL', 'SQLite', 'Oracle', 'SQL Server'])
    host = st.text_input("Hôte de la base de données", "localhost")
    user = st.text_input("Utilisateur de la base de données", "root")
    password = st.text_input("Mot de passe de la base de données", "12345678", type="password")
    database = st.text_input("Nom de la base de données", "ADMIN")

    if st.button("Obtenir la volumétrie"):
        connection = connect_to_db(db_type, host, user, password, database)

        if connection:
            try:
                cursor = connection.cursor()

                # Requêtes de volumétrie adaptées en fonction du type de base de données
                if db_type in ['MySQL', 'PostgreSQL']:
                    cursor.execute(""" 
                        SELECT table_name, 
                               ROUND((data_length + index_length) / 1024 / 1024, 2) AS size_in_MB 
                        FROM information_schema.tables 
                        WHERE table_schema = %s;
                    """, (database,))
                    results = cursor.fetchall()
                    st.write("Volumétrie des tables :")
                    for table in results:
                        st.write(f"Table: {table[0]}, Taille: {table[1]} MB")
                elif db_type == 'SQLite':
                    cursor.execute("""
                        SELECT name, 
                               ROUND(sum(pgsize) / 1024 / 1024, 2) AS size_in_MB
                        FROM dbstat
                        GROUP BY name;
                    """)
                    results = cursor.fetchall()
                    st.write("Volumétrie des tables :")
                    for table in results:
                        st.write(f"Table: {table[0]}, Taille: {table[1]} MB")
                elif db_type == 'Oracle':
                    cursor.execute("""
                        SELECT table_name, 
                               ROUND(SUM(bytes) / 1024 / 1024, 2) AS size_in_MB 
                        FROM user_segments
                        GROUP BY table_name;
                    """)
                    results = cursor.fetchall()
                    st.write("Volumétrie des tables :")
                    for table in results:
                        st.write(f"Table: {table[0]}, Taille: {table[1]} MB")
                elif db_type == 'SQL Server':
                    cursor.execute("""
                        SELECT table_name, 
                               SUM(reserved_page_count) * 8 / 1024 AS size_in_MB
                        FROM sys.dm_db_partition_stats
                        JOIN information_schema.tables 
                        ON sys.dm_db_partition_stats.object_id = object_id(tables.table_name)
                        GROUP BY table_name;
                    """)
                    results = cursor.fetchall()
                    st.write("Volumétrie des tables :")
                    for table in results:
                        st.write(f"Table: {table[0]}, Taille: {table[1]} MB")

                # Calcul de la taille totale de la base de données
                if db_type in ['MySQL', 'PostgreSQL']:
                    cursor.execute("""
                        SELECT ROUND(SUM(data_length + index_length) / 1024 / 1024, 2) AS total_size_in_MB
                        FROM information_schema.tables 
                        WHERE table_schema = %s;
                    """, (database,))
                elif db_type == 'SQLite':
                    cursor.execute("""
                        SELECT ROUND(SUM(pgsize) / 1024 / 1024, 2) AS total_size_in_MB
                        FROM dbstat;
                    """)
                elif db_type == 'Oracle':
                    cursor.execute("""
                        SELECT ROUND(SUM(bytes) / 1024 / 1024, 2) AS total_size_in_MB
                        FROM user_segments;
                    """)
                elif db_type == 'SQL Server':
                    cursor.execute("""
                        SELECT SUM(reserved_page_count) * 8 / 1024 AS total_size_in_MB
                        FROM sys.dm_db_partition_stats;
                    """)
                total_size = cursor.fetchone()
                st.write(f"Taille totale de la base : {total_size[0]} MB")

            except Exception as e:
                st.error(f"Erreur dans la récupération des données: {str(e)}")
            finally:
                connection.close()
                st.success("Connexion à la base fermée")

elif data_source == 'Fichier':
    # Entrée de fichier
    st.subheader("Télécharger un Fichier")
    uploaded_file = st.file_uploader("Téléchargez un fichier CSV ou Excel", type=["csv", "xlsx"])

    if uploaded_file is not None:
        process_large_file(uploaded_file)  # Appeler la fonction pour traiter les fichiers volumineux
