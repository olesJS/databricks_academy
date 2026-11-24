import io
import json
import logging
import time
from datetime import datetime, timezone

import requests
from databricks.sdk import WorkspaceClient
from databricks.sdk.errors import NotFound
from databricks.sdk.service.catalog import VolumeType

PROFILE = "uni_free"
CATALOG = "workspace"
SCHEMA = "f1_rag_bronze"
VOLUME = "raw"
VOLUME_DIR = f"/Volumes/{CATALOG}/{SCHEMA}/{VOLUME}/wiki"

API_URL = "https://en.wikipedia.org/w/api.php"
USER_AGENT = "databricks-academy-lab12-rag-assistant/1.0 (https://github.com/olesJS/databricks_academy; educational project)"
REQUEST_DELAY_SECONDS = 0.5
MAX_RETRIES = 3


# category -> Wikipedia titles
ARTICLES = {
    "driver": [
        # current grid
        "Max Verstappen", "Lewis Hamilton", "Charles Leclerc", "Lando Norris", "Oscar Piastri",
        "George Russell (racing driver)", "Carlos Sainz Jr.", "Fernando Alonso", "Sergio Pérez",
        "Lance Stroll", "Pierre Gasly", "Esteban Ocon", "Alexander Albon", "Yuki Tsunoda",
        "Nico Hülkenberg", "Kevin Magnussen", "Valtteri Bottas", "Zhou Guanyu", "Daniel Ricciardo",
        "Oliver Bearman", "Franco Colapinto", "Liam Lawson", "Andrea Kimi Antonelli",
        "Isack Hadjar", "Gabriel Bortoleto",
        # champions and legends
        "Sebastian Vettel", "Kimi Räikkönen", "Nico Rosberg", "Michael Schumacher", "Ayrton Senna",
        "Alain Prost", "Niki Lauda", "Jenson Button", "Mika Häkkinen", "Jackie Stewart",
        "Juan Manuel Fangio", "Jim Clark", "Nigel Mansell", "Damon Hill", "Nelson Piquet",
    ],
    "team": [
        "Scuderia Ferrari", "Mercedes-Benz in Formula One", "Red Bull Racing", "McLaren",
        "Aston Martin in Formula One", "Alpine F1 Team", "Williams Racing", "Racing Bulls",
        "Sauber Motorsport", "Haas F1 Team",
    ],
    "circuit": [
        "Bahrain International Circuit", "Jeddah Corniche Circuit", "Albert Park Circuit",
        "Suzuka International Racing Course", "Shanghai International Circuit",
        "Miami International Autodrome", "Imola Circuit", "Circuit de Monaco",
        "Circuit Gilles Villeneuve", "Circuit de Barcelona-Catalunya", "Red Bull Ring",
        "Silverstone Circuit", "Hungaroring", "Circuit de Spa-Francorchamps", "Circuit Zandvoort",
        "Monza Circuit", "Baku City Circuit", "Marina Bay Street Circuit",
        "Circuit of the Americas", "Autódromo Hermanos Rodríguez", "Interlagos Circuit",
        "Las Vegas Strip Circuit", "Lusail International Circuit", "Yas Marina Circuit",
    ],
    "season": [
        f"{year} Formula One World Championship" for year in range(2020, 2026)
    ],
    "general": [
        "Formula One", "History of Formula One", "Formula One regulations",
        "Formula One car", "Drag reduction system", "Formula One tyres",
    ],
}

logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
log = logging.getLogger("fetch_wiki")


def ensure_volume(w: WorkspaceClient) -> None:
    try:
        w.schemas.get(f"{CATALOG}.{SCHEMA}")
    except NotFound:
        w.schemas.create(name=SCHEMA, catalog_name=CATALOG)
        log.info("Created schema %s.%s", CATALOG, SCHEMA)

    try:
        w.volumes.read(f"{CATALOG}.{SCHEMA}.{VOLUME}")
    except NotFound:
        w.volumes.create(catalog_name=CATALOG, schema_name=SCHEMA, name=VOLUME,
                         volume_type=VolumeType.MANAGED)
        log.info("Created volume %s.%s.%s", CATALOG, SCHEMA, VOLUME)


def fetch_page(session: requests.Session, title: str) -> dict | None:
    params = {
        "action": "query",
        "format": "json",
        "formatversion": 2,
        "titles": title,
        "redirects": 1,
        "prop": "extracts|revisions|info",
        "explaintext": 1,           # plain text instead of HTML
        "exsectionformat": "wiki",  # keep headings as "== Heading =="
        "rvprop": "ids",
        "inprop": "url",
    }

    for attempt in range(1, MAX_RETRIES + 1):
        response = session.get(API_URL, params=params, timeout=30)
        if response.status_code == 200:
            break
        log.warning("%s: HTTP %s (attempt %d/%d)", title, response.status_code, attempt, MAX_RETRIES)
        time.sleep(2 ** attempt)
    else:
        response.raise_for_status()

    page = response.json()["query"]["pages"][0]
    if page.get("missing") or not page.get("extract"):
        return None

    return {
        "page_id": page["pageid"],
        "title": page["title"],
        "requested_title": title,
        "url": page["fullurl"],
        "revision_id": page["revisions"][0]["revid"],
        "text": page["extract"],
    }


def upload_page(w: WorkspaceClient, page: dict) -> str:
    path = f"{VOLUME_DIR}/{page['category']}__{page['page_id']}.json"
    content = json.dumps(page, ensure_ascii=False, indent=2).encode("utf-8")
    w.files.upload(path, io.BytesIO(content), overwrite=True)
    return path


def main() -> None:
    w = WorkspaceClient(profile=PROFILE)
    ensure_volume(w)

    session = requests.Session()
    session.headers["User-Agent"] = USER_AGENT

    saved, missing = 0, []
    seen_page_ids = set()

    for category, titles in ARTICLES.items():
        for title in titles:
            page = fetch_page(session, title)
            time.sleep(REQUEST_DELAY_SECONDS)

            if page is None:
                log.warning("Not found: %s", title)
                missing.append(title)
                continue
            if page["page_id"] in seen_page_ids:
                log.warning("Duplicate page (redirect): %s -> %s", title, page["title"])
                continue
            seen_page_ids.add(page["page_id"])

            page["category"] = category
            page["fetched_at"] = datetime.now(timezone.utc).isoformat()

            upload_page(w, page)
            saved += 1
            log.info("Uploaded %-8s %-45s %6d chars", category, page["title"], len(page["text"]))

    log.info("Done: uploaded %d pages to %s", saved, VOLUME_DIR)
    if missing:
        log.warning("Not found (%d): %s", len(missing), ", ".join(missing))


if __name__ == "__main__":
    main()
