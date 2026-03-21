"""
Warehouse context queries for gamefeed content enrichment.

Pulls from the production schema to add analytical depth to content cards.
Every function returns a plain dict or string — ready to drop onto a card.

Ported from gamefeed/scripts/warehouse.py so both projects share one copy.
"""

from __future__ import annotations
from functools import lru_cache

from sqlalchemy import create_engine, text
from utils.utils import build_db_url

_engine = None

def _get_engine():
    global _engine
    if _engine is None:
        _engine = create_engine(build_db_url(), pool_pre_ping=True)
    return _engine


def _query_one(sql: str, params: dict = None) -> dict | None:
    with _get_engine().connect() as conn:
        row = conn.execute(text(sql), params or {}).fetchone()
        if row is None:
            return None
        return dict(row._mapping)


def _query_all(sql: str, params: dict = None) -> list[dict]:
    with _get_engine().connect() as conn:
        rows = conn.execute(text(sql), params or {}).fetchall()
        return [dict(r._mapping) for r in rows]


# ── Batter context ───────────────────────────────────────────────────

def batter_ev_percentile(batter_id: int, exit_velo: float) -> dict | None:
    """Where does this EV rank among all this batter's batted balls?"""
    sql = """
        SELECT
            COUNT(*) AS total_bbs,
            SUM(CASE WHEN bb.launch_speed <= :ev THEN 1 ELSE 0 END) AS below_count,
            MAX(bb.launch_speed) AS career_max_ev,
            ROUND(AVG(bb.launch_speed)::numeric, 1) AS avg_ev
        FROM production.sat_batted_balls bb
        JOIN production.fact_pitch fp ON fp.pitch_id = bb.pitch_id
        WHERE fp.batter_id = :batter_id
          AND bb.launch_speed IS NOT NULL
          AND bb.launch_speed != 'NaN'::real
    """
    row = _query_one(sql, {"batter_id": batter_id, "ev": exit_velo})
    if not row or not row["total_bbs"]:
        return None
    total = row["total_bbs"]
    pctile = round(row["below_count"] / total * 100, 1)
    return {
        "percentile": pctile,
        "total_batted_balls": total,
        "career_max_ev": float(row["career_max_ev"]) if row["career_max_ev"] else None,
        "avg_ev": float(row["avg_ev"]) if row["avg_ev"] else None,
    }


def batter_season_batted_ball_stats(batter_id: int, season: int = 2025) -> dict | None:
    """Batter's barrel rate, hard hit rate, avg EV for a season."""
    sql = """
        SELECT
            COUNT(*) AS total_bbs,
            ROUND(AVG(bb.launch_speed)::numeric, 1) AS avg_ev,
            ROUND(AVG(CASE WHEN bb.hard_hit THEN 1 ELSE 0 END)::numeric, 3) AS hard_hit_rate,
            ROUND(AVG(CASE WHEN bb.launch_speed >= 98
                AND bb.launch_angle BETWEEN
                    GREATEST(26 - (bb.launch_speed - 98), 8)
                    AND LEAST(30 + (bb.launch_speed - 98) * 1.2, 50)
                THEN 1 ELSE 0 END)::numeric, 3) AS barrel_rate,
            ROUND(AVG(CASE WHEN bb.xwoba != 'NaN'::real THEN bb.xwoba END)::numeric, 3) AS avg_xwoba
        FROM production.sat_batted_balls bb
        JOIN production.fact_pitch fp ON fp.pitch_id = bb.pitch_id
        JOIN production.dim_game g ON g.game_pk = fp.game_pk
        WHERE fp.batter_id = :batter_id
          AND g.season = :season
          AND bb.launch_speed IS NOT NULL
          AND bb.launch_speed != 'NaN'::real
    """
    return _query_one(sql, {"batter_id": batter_id, "season": season})


# ── Pitcher context ──────────────────────────────────────────────────

def pitcher_season_stats(pitcher_id: int, season: int = 2025) -> dict | None:
    """Pitcher's K%, BB%, whiff%, avg velo for a season."""
    sql = """
        WITH pa_stats AS (
            SELECT
                COUNT(*) AS total_pa,
                SUM(CASE WHEN pa.events = 'strikeout' THEN 1 ELSE 0 END) AS k_count,
                SUM(CASE WHEN pa.events = 'walk' THEN 1 ELSE 0 END) AS bb_count
            FROM production.fact_pa pa
            JOIN production.dim_game g ON g.game_pk = pa.game_pk
            WHERE pa.pitcher_id = :pitcher_id AND g.season = :season
        ),
        pitch_stats AS (
            SELECT
                COUNT(*) AS total_pitches,
                ROUND(AVG(CASE WHEN fp.release_speed != 'NaN'::real THEN fp.release_speed END)::numeric, 1) AS avg_velo,
                ROUND(AVG(CASE WHEN fp.is_whiff THEN 1 ELSE 0 END)::numeric, 3) AS whiff_rate,
                ROUND(AVG(CASE WHEN fp.is_called_strike OR fp.is_whiff
                    THEN 1 ELSE 0 END)::numeric, 3) AS csw_rate
            FROM production.fact_pitch fp
            JOIN production.dim_game g ON g.game_pk = fp.game_pk
            WHERE fp.pitcher_id = :pitcher_id AND g.season = :season
        )
        SELECT
            pa.total_pa,
            pa.k_count,
            pa.bb_count,
            ROUND(pa.k_count::numeric / NULLIF(pa.total_pa, 0), 3) AS k_rate,
            ROUND(pa.bb_count::numeric / NULLIF(pa.total_pa, 0), 3) AS bb_rate,
            p.total_pitches,
            p.avg_velo,
            p.whiff_rate,
            p.csw_rate
        FROM pa_stats pa, pitch_stats p
    """
    return _query_one(sql, {"pitcher_id": pitcher_id, "season": season})


def pitcher_velo_by_pitch_type(pitcher_id: int, season: int = 2025) -> list[dict]:
    """Pitcher's avg velo per pitch type for comparison to today's game."""
    sql = """
        SELECT
            fp.pitch_type,
            fp.pitch_name,
            COUNT(*) AS pitches,
            ROUND(AVG(CASE WHEN fp.release_speed != 'NaN'::real THEN fp.release_speed END)::numeric, 1) AS avg_velo,
            ROUND(AVG(CASE WHEN fp.is_whiff THEN 1 ELSE 0 END)::numeric, 3) AS whiff_rate
        FROM production.fact_pitch fp
        JOIN production.dim_game g ON g.game_pk = fp.game_pk
        WHERE fp.pitcher_id = :pitcher_id
          AND g.season = :season
          AND fp.pitch_type IS NOT NULL
        GROUP BY fp.pitch_type, fp.pitch_name
        HAVING COUNT(*) >= 20
        ORDER BY COUNT(*) DESC
    """
    return _query_all(sql, {"pitcher_id": pitcher_id, "season": season})


def pitch_type_league_xwoba(pitch_type: str, season: int = 2025) -> dict | None:
    """League-wide xwOBA against a pitch type."""
    sql = """
        SELECT
            COUNT(*) AS total_bip,
            ROUND(AVG(bb.xwoba)::numeric, 3) AS league_xwoba
        FROM production.sat_batted_balls bb
        JOIN production.fact_pitch fp ON fp.pitch_id = bb.pitch_id
        JOIN production.dim_game g ON g.game_pk = fp.game_pk
        WHERE fp.pitch_type = :pitch_type
          AND g.season = :season
          AND bb.xwoba IS NOT NULL
          AND bb.xwoba != 'NaN'::real
    """
    return _query_one(sql, {"pitch_type": pitch_type, "season": season})


# ── At-bat context ───────────────────────────────────────────────────

def batter_vs_pitch_type(batter_id: int, pitch_code: str, season: int = 2025) -> dict | None:
    """Batter's whiff rate and result stats against a specific pitch type."""
    sql = """
        SELECT
            COUNT(*) AS pitches_seen,
            ROUND(AVG(CASE WHEN fp.is_whiff THEN 1 ELSE 0 END)::numeric, 3) AS whiff_rate,
            ROUND(AVG(CASE WHEN fp.is_swing THEN 1 ELSE 0 END)::numeric, 3) AS swing_rate,
            ROUND(AVG(CASE WHEN fp.is_called_strike THEN 1 ELSE 0 END)::numeric, 3) AS called_strike_rate
        FROM production.fact_pitch fp
        JOIN production.dim_game g ON g.game_pk = fp.game_pk
        WHERE fp.batter_id = :batter_id
          AND fp.pitch_type = :pitch_code
          AND g.season = :season
    """
    return _query_one(sql, {"batter_id": batter_id, "pitch_code": pitch_code, "season": season})


def pitcher_pitch_type_stats(pitcher_id: int, pitch_code: str, season: int = 2025) -> dict | None:
    """Pitcher's usage and effectiveness for a specific pitch type."""
    sql = """
        SELECT
            COUNT(*) AS total_thrown,
            ROUND(AVG(CASE WHEN fp.release_speed != 'NaN'::real THEN fp.release_speed END)::numeric, 1) AS avg_velo,
            ROUND(AVG(CASE WHEN fp.is_whiff THEN 1 ELSE 0 END)::numeric, 3) AS whiff_rate,
            ROUND(AVG(CASE WHEN fp.is_called_strike OR fp.is_whiff
                THEN 1 ELSE 0 END)::numeric, 3) AS csw_rate
        FROM production.fact_pitch fp
        JOIN production.dim_game g ON g.game_pk = fp.game_pk
        WHERE fp.pitcher_id = :pitcher_id
          AND fp.pitch_type = :pitch_code
          AND g.season = :season
    """
    return _query_one(sql, {"pitcher_id": pitcher_id, "pitch_code": pitch_code, "season": season})


def format_at_bat_context(batter_name: str, batter_id: int,
                          pitcher_name: str, pitcher_id: int,
                          final_pitch_code: str, season: int = 2025) -> str | None:
    """One-liner context for an at-bat based on the strikeout/put-away pitch."""
    batter_stats = batter_vs_pitch_type(batter_id, final_pitch_code, season)
    pitcher_stats = pitcher_pitch_type_stats(pitcher_id, final_pitch_code, season)

    parts = []
    if batter_stats and batter_stats.get("pitches_seen") and batter_stats["pitches_seen"] >= 30:
        whiff = float(batter_stats["whiff_rate"]) * 100
        parts.append(f"{batter_name} whiffs {whiff:.0f}% vs {final_pitch_code}")

    if pitcher_stats and pitcher_stats.get("total_thrown") and pitcher_stats["total_thrown"] >= 50:
        csw = float(pitcher_stats["csw_rate"]) * 100
        parts.append(f"{pitcher_name} {final_pitch_code} CSW: {csw:.0f}%")

    return "  |  ".join(parts) if parts else None


# ── Leaderboard queries ──────────────────────────────────────────────

def season_k_rate_leaders(season: int = 2025, min_pa: int = 200) -> list[dict]:
    """Top K% pitchers for a season (for ranking context)."""
    sql = """
        SELECT
            pa.pitcher_id,
            dp.player_name,
            COUNT(*) AS total_pa,
            SUM(CASE WHEN pa.events = 'strikeout' THEN 1 ELSE 0 END) AS k_count,
            ROUND(SUM(CASE WHEN pa.events = 'strikeout' THEN 1 ELSE 0 END)::numeric
                / COUNT(*)::numeric, 3) AS k_rate
        FROM production.fact_pa pa
        JOIN production.dim_game g ON g.game_pk = pa.game_pk
        JOIN production.dim_player dp ON dp.player_id = pa.pitcher_id
        WHERE g.season = :season
        GROUP BY pa.pitcher_id, dp.player_name
        HAVING COUNT(*) >= :min_pa
        ORDER BY k_rate DESC
    """
    return _query_all(sql, {"season": season, "min_pa": min_pa})


def season_barrel_rate_leaders(season: int = 2025, min_bb: int = 100) -> list[dict]:
    """Top barrel rate batters for a season."""
    sql = """
        SELECT
            fp.batter_id,
            dp.player_name,
            COUNT(*) AS total_bbs,
            ROUND(AVG(bb.launch_speed)::numeric, 1) AS avg_ev,
            ROUND(AVG(CASE WHEN bb.launch_speed >= 98
                AND bb.launch_angle BETWEEN
                    GREATEST(26 - (bb.launch_speed - 98), 8)
                    AND LEAST(30 + (bb.launch_speed - 98) * 1.2, 50)
                THEN 1 ELSE 0 END)::numeric, 3) AS barrel_rate
        FROM production.sat_batted_balls bb
        JOIN production.fact_pitch fp ON fp.pitch_id = bb.pitch_id
        JOIN production.dim_game g ON g.game_pk = fp.game_pk
        JOIN production.dim_player dp ON dp.player_id = fp.batter_id
        WHERE g.season = :season
          AND bb.launch_speed IS NOT NULL
          AND bb.launch_speed != 'NaN'::real
        GROUP BY fp.batter_id, dp.player_name
        HAVING COUNT(*) >= :min_bb
        ORDER BY barrel_rate DESC
    """
    return _query_all(sql, {"season": season, "min_bb": min_bb})


# ── Context line formatters ──────────────────────────────────────────

def pitcher_season_movement(pitcher_id: int, season: int = 2025) -> list[dict]:
    """Per-pitch movement data for a pitcher's season."""
    sql = """
        SELECT
            fp.pitch_type,
            fp.pitch_name,
            ps.pfx_x * 12 AS pfx_x,
            ps.pfx_z * 12 AS pfx_z,
            ps.release_speed,
            ps.release_spin_rate
        FROM production.fact_pitch fp
        JOIN production.sat_pitch_shape ps ON ps.pitch_id = fp.pitch_id
        JOIN production.dim_game g ON g.game_pk = fp.game_pk
        WHERE fp.pitcher_id = :pitcher_id
          AND g.season = :season
          AND fp.pitch_type IS NOT NULL
          AND ps.pfx_x != 'NaN'::real
          AND ps.pfx_z != 'NaN'::real
    """
    return _query_all(sql, {"pitcher_id": pitcher_id, "season": season})


def pitcher_season_movement_avgs(pitcher_id: int, season: int = 2025) -> list[dict]:
    """Per-pitch-type average movement for a pitcher's season."""
    sql = """
        SELECT
            fp.pitch_type,
            fp.pitch_name,
            COUNT(*) AS count,
            ROUND((AVG(ps.pfx_x) * 12)::numeric, 1) AS avg_pfx_x,
            ROUND((AVG(ps.pfx_z) * 12)::numeric, 1) AS avg_pfx_z,
            ROUND(AVG(CASE WHEN ps.release_speed != 'NaN'::real
                THEN ps.release_speed END)::numeric, 1) AS avg_velo,
            ROUND(AVG(CASE WHEN ps.release_spin_rate != 'NaN'::real
                THEN ps.release_spin_rate END)::numeric, 0) AS avg_spin
        FROM production.fact_pitch fp
        JOIN production.sat_pitch_shape ps ON ps.pitch_id = fp.pitch_id
        JOIN production.dim_game g ON g.game_pk = fp.game_pk
        WHERE fp.pitcher_id = :pitcher_id
          AND g.season = :season
          AND fp.pitch_type IS NOT NULL
          AND ps.pfx_x != 'NaN'::real
          AND ps.pfx_z != 'NaN'::real
        GROUP BY fp.pitch_type, fp.pitch_name
        HAVING COUNT(*) >= 10
        ORDER BY COUNT(*) DESC
    """
    return _query_all(sql, {"pitcher_id": pitcher_id, "season": season})


def format_batter_ev_context(batter_name: str, batter_id: int, exit_velo: float) -> str | None:
    """One-liner context for a batter's exit velo."""
    ctx = batter_ev_percentile(batter_id, exit_velo)
    if not ctx:
        return None
    pct = ctx["percentile"]
    max_ev = ctx["career_max_ev"]
    if max_ev and exit_velo >= max_ev:
        return f"Career-high exit velo for {batter_name} (prev max: {max_ev:.1f} mph)"
    if pct >= 99:
        return f"Top 1% exit velo for {batter_name} (avg EV: {ctx['avg_ev']} mph)"
    if pct >= 95:
        return f"{pct}th percentile exit velo for {batter_name} (avg EV: {ctx['avg_ev']} mph)"
    return None


def format_pitcher_context(pitcher_name: str, pitcher_id: int, season: int = 2025) -> str | None:
    """One-liner context for a pitcher's season performance."""
    stats = pitcher_season_stats(pitcher_id, season)
    if not stats or not stats.get("total_pa"):
        return None
    k_rate = stats["k_rate"]
    whiff = stats["whiff_rate"]
    if k_rate and float(k_rate) > 0:
        k_pct = float(k_rate) * 100
        whiff_pct = float(whiff) * 100 if whiff else 0
        return f"{pitcher_name} {season} season: {k_pct:.1f}% K rate | {whiff_pct:.1f}% whiff rate | {stats['avg_velo']} mph avg"
    return None
