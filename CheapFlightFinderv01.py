import pandas as pd
import requests
import logging
from pymongo import MongoClient
from bson import ObjectId

# Logger ayarları
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

# MongoDB bağlantı ayarları
username = "jhaaz"
password = "Jhaazhum2019!"
host = "191.96.31.122"
port = 27017
database = "scraper"
mongo_uri = f"mongodb://{username}:{password}@{host}:{port}/{database}"

# Google Sheets URL (CSV formatında)
sheet_url = "https://docs.google.com/spreadsheets/d/1uAs-T5COeSGpaeRr3_ywytoO9vbhubDP1S_Ctfpy-Lc/export?format=csv"

# CSV verisini çekme
df = pd.read_csv(sheet_url)

# MongoDB'ye bağlanın
client = MongoClient(mongo_uri, serverSelectionTimeoutMS=5000)
db = client[database]
collection = db["flights"]

# Bağlantı testi
try:
    client.server_info()  # Sunucu bilgilerini alarak bağlantıyı test edin
    logger.info("MongoDB'ye başarıyla bağlanıldı.")
except Exception as e:
    logger.error(f"MongoDB bağlantısı hatası: {e}")
    exit()

# Google Sheets'ten alınan verileri işleyin
for index, row in df.iterrows():
    start_date = row['Start Date']  # Başlangıç tarihini al
    return_date = row['Return']  # Dönüş tarihini al
    additional_day = row['Additional Day']  # Ekstra gün sayısını al

    # Seyahat süresi aralığını hesapla (return - start + additional day)
    trip_duration_range = (pd.to_datetime(return_date) - pd.to_datetime(start_date)).days + additional_day

    # URL'yi dinamik olarak oluşturun
    json_url = f"https://www.momondo.com.tr/s/horizon/exploreapi/destinations?airport=IST&budget=&depart={start_date.replace('-', '')}&return={return_date.replace('-', '')}&duration=&exactDates=true&flightMaxStops=&stopsFilterActive=false&topRightLat=55.750402238022794&topRightLon=54.27336549316409&bottomLeftLat=22.713018052794585&bottomLeftLon=3.208912368164092&zoomLevel=5&selectedMarker=&themeCode=&selectedDestination=&searchedDestination=anywhere&xsSupport=false"

    logger.info(f"İşleniyor: {start_date} - {return_date}")
    logger.info(f"API URL: {json_url}")

    try:
        # JSON verilerini çekin
        response = requests.get(json_url)

        # HTTP hatasını kontrol et
        response.raise_for_status()

        # Yanıtın boş olup olmadığını kontrol et
        if not response.text.strip():  # Eğer yanıt boşsa
            logger.error(f"Boş yanıt alındı: {json_url}")
            continue  # Bir sonraki satıra geç

        # JSON verilerini parse edin
        try:
            json_data = response.json()
        except ValueError as e:
            logger.error(f"JSON parsing hatası: {e} - {json_url}")
            logger.error(f"Yanıt: {response.text}")  # Yanıtı loglayarak hatayı daha iyi anlayabilirsiniz
            continue  # Bir sonraki satıra geç

        # MongoDB'ye verileri toplu ekleme
        bulk_insert = []

        for destination in json_data.get("destinations", []):
            document = {
                "_id": ObjectId(),
                "Destination": f"{destination.get('city', {}).get('name', '')}, {destination.get('country', {}).get('name', '')}",
                "City": destination.get("city", {}).get("name", ""),
                "Country": destination.get("country", {}).get("name", ""),
                "Price": destination.get("flightInfo", {}).get("price", "Bilinmiyor"),
                "Stops": destination.get("flightMaxStops", 0),
                "Departure Date": destination.get("departd", "Bilinmiyor"),
                "Return Date": destination.get("returnd", "Bilinmiyor"),
                "Flight Duration": destination.get("flightMaxDuration", 0),
                "Search URL": f"https://www.momondo.com.tr{destination.get('clickoutUrl', '')}",
                "Days": destination.get("days", 0)  # 'Days' verisini ekliyoruz
            }

            bulk_insert.append(document)

        # Eğer liste dolarsa, toplu olarak MongoDB'ye ekle
        if bulk_insert:
            try:
                collection.insert_many(bulk_insert, ordered=False)  # ordered=False, hata durumunda devam etmeyi sağlar
                logger.info(f"Toplamda {len(bulk_insert)} uçuş kaydedildi.")
            except Exception as e:
                logger.error(f"Toplu insert işlemi sırasında hata: {e}")

    except requests.exceptions.RequestException as e:
        logger.error(f"API isteği hatası: {e} - {json_url}")
        continue  # Bir sonraki satıra geç

logger.info("Tüm veriler MongoDB'ye kaydedildi.")
