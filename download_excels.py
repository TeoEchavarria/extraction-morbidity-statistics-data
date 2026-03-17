import os
import requests
from bs4 import BeautifulSoup
from urllib.parse import urljoin

# 🔧 CONFIG
BASE_URL = "https://dssa.gov.co/OSSSA/Estad%C3%ADsticas%20Morbilidad.html"
OUTPUT_DIR = "excels"

os.makedirs(OUTPUT_DIR, exist_ok=True)

# 📥 Descargar HTML desde la web
print(f"🌐 Descargando HTML desde: {BASE_URL}")
response = requests.get(BASE_URL, timeout=15)
response.raise_for_status()
html = response.text

soup = BeautifulSoup(html, "html.parser")

# 🎯 Encontrar todos los links
links = soup.find_all("a", href=True)

excel_links = []

for link in links:
    href = link["href"]

    if href.endswith((".xls", ".xlsx")):
        full_url = urljoin(BASE_URL, href)
        excel_links.append(full_url)

print(f"📊 Encontrados {len(excel_links)} archivos Excel")

# 🚀 Descargar archivos
for url in excel_links:
    try:
        filename = url.split("/")[-1]
        filepath = os.path.join(OUTPUT_DIR, filename)

        print(f"⬇️ Descargando: {filename}")

        response = requests.get(url, timeout=15)
        response.raise_for_status()

        with open(filepath, "wb") as f:
            f.write(response.content)

    except Exception as e:
        print(f"❌ Error con {url}: {e}")

print("✅ Descarga completa")