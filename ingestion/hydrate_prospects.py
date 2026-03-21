import logging
from sqlalchemy import create_engine, text
from utils.utils import build_db_url
from utils.retry import build_retry_session

logger = logging.getLogger(__name__)
session = build_retry_session(timeout=30)
engine = create_engine(build_db_url(database='mlb_fantasy'), pool_pre_ping=True)

PEOPLE_URL = "https://statsapi.mlb.com/api/v1/people"


def _fetch_people_bulk(player_ids: list[int]) -> dict[int, dict]:
    """Fetch person details for up to 200 player IDs in one API call."""
    ids_str = ",".join(str(pid) for pid in player_ids)
    url = f"{PEOPLE_URL}?personIds={ids_str}&hydrate=currentTeam"
    resp = session.get(url, timeout=session.timeout)
    resp.raise_for_status()

    result = {}
    for person in resp.json().get('people', []):
        pid = person.get('id')
        if pid is None:
            continue
        result[pid] = {
            'first_name': person.get('firstName'),
            'last_name': person.get('lastName'),
            'bat_side': (person.get('batSide') or {}).get('code'),
            'pitch_hand': (person.get('pitchHand') or {}).get('code'),
            'birth_date': person.get('birthDate'),
            'current_age': person.get('currentAge'),
            'height': person.get('height'),
            'weight': person.get('weight'),
            'mlb_debut_date': person.get('mlbDebutDate'),
            'draft_year': person.get('draftYear'),
        }
    return result


def hydrate_prospects(batch_size: int = 200):
    """Fetch bio data from MLB API and update dim_prospects rows missing birth_date."""
    with engine.begin() as conn:
        rows = conn.execute(text(
            "SELECT DISTINCT player_id FROM production.dim_prospects "
            "WHERE birth_date IS NULL ORDER BY player_id"
        )).fetchall()

    player_ids = [r[0] for r in rows]
    logger.info(f"Found {len(player_ids)} players needing hydration")

    update_sql = text("""
        UPDATE production.dim_prospects SET
            first_name = :first_name,
            last_name = :last_name,
            bat_side = :bat_side,
            pitch_hand = :pitch_hand,
            birth_date = CAST(:birth_date AS date),
            current_age = :current_age,
            height = :height,
            weight = :weight,
            mlb_debut_date = CAST(:mlb_debut_date AS date),
            draft_year = :draft_year,
            updated_at = now()
        WHERE player_id = :player_id
    """)

    total_updated = 0
    for i in range(0, len(player_ids), batch_size):
        batch = player_ids[i:i + batch_size]
        try:
            people = _fetch_people_bulk(batch)
        except Exception as exc:
            logger.error(f"Failed batch at offset {i}: {exc}")
            continue

        with engine.begin() as conn:
            for pid, info in people.items():
                info['player_id'] = pid
                conn.execute(update_sql, info)
                total_updated += 1

        if (i + batch_size) % 1000 < batch_size:
            logger.info(f"Hydrated {min(i + batch_size, len(player_ids))}/{len(player_ids)} "
                        f"({total_updated} updated)")

    logger.info(f"Done: hydrated {total_updated} players out of {len(player_ids)}")


if __name__ == '__main__':
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s [%(levelname)s] %(name)s: %(message)s",
    )
    hydrate_prospects()
