import pandas as pd
import matplotlib.pyplot as plt
import seaborn as sns
from sqlalchemy import create_engine
from utils.utils import build_db_url

sql_view = '''
    WITH pitch_level AS (
        SELECT
            ps.pitch_id,
            ps.pa_id,
            ps.pitcher_id,
            pa.times_through_order,
            bb.xwoba,
            ps.is_whiff,
            ps.is_swing,
            ps.is_inplay_contact
        FROM production.v_pitch_shape_features ps
        LEFT JOIN production.sat_batted_balls bb
            ON ps.pitch_id = bb.pitch_id
        LEFT JOIN production.fact_pa pa
            ON ps.pa_id = pa.pa_id
        WHERE ps.pitcher_id = 808967 AND pa.times_through_order < 4
    ),
    balls_in_play AS (
        SELECT
            pitcher_id,
            times_through_order,
            ROUND(
                AVG(NULLIF(xwoba::float8, 'NaN'::float8))::numeric,
                3
            ) AS avg_xwoba_on_bip
        FROM pitch_level 
        WHERE is_inplay_contact IS TRUE
        GROUP BY 1,2
    ),
    agg AS (
        SELECT
            p.pitcher_id,
            p.times_through_order,
            ROUND(
                AVG((p.is_whiff::int)) FILTER(WHERE p.is_swing IS TRUE)::numeric, 
                3
            ) AS whiff_rate_per_swing,
            COUNT(*)::bigint AS pitches_in_order
        FROM pitch_level p
        GROUP BY 1,2
    )
    SELECT 
        a.pitcher_id,
        a.times_through_order,
        bip.avg_xwoba_on_bip,
        a.whiff_rate_per_swing,
        a.pitches_in_order
    FROM agg a
    LEFT JOIN balls_in_play bip
        ON a.pitcher_id = bip.pitcher_id
        AND a.times_through_order = bip.times_through_order
'''

engine = create_engine(build_db_url())

with engine.begin() as conn:
    df = pd.read_sql(sql_view, conn)

baseline = df['avg_xwoba_on_bip'].mean()

# Clean style without grid
sns.set_theme(style="ticks", font_scale=1.1)
palette_bar = "#4C78A8"
palette_line = "#F58518"

fig, ax1 = plt.subplots(figsize=(9, 6))

# Bars
x_pos = [0, 1, 2]
bars = ax1.bar(
    x_pos,
    df['avg_xwoba_on_bip'].values,
    color=palette_bar,
    alpha=0.8,
    width=0.6
)

# Value labels on bars
for i, (bar, val) in enumerate(zip(bars, df['avg_xwoba_on_bip'].values)):
    ax1.text(
        bar.get_x() + bar.get_width()/2,
        val + 0.005,
        f'{val:.3f}',
        ha='center',
        va='bottom',
        fontsize=10,
        fontweight='medium'
    )

# Subtle baseline
ax1.axhline(baseline, color='gray', linestyle=':', linewidth=1.5, alpha=0.6, zorder=0)

ax1.set_ylabel("Avg xwOBA on BIP", color=palette_bar, fontweight='medium')
ax1.set_xlabel("Times Through the Order", fontweight='medium')
ax1.set_title("Yamamoto: Effectiveness by Times Through the Order", 
              pad=15, fontsize=13, fontweight='bold')
ax1.set_xticks(x_pos)
ax1.set_xticklabels(['1st', '2nd', '3rd'])
ax1.set_ylim(0, max(df['avg_xwoba_on_bip']) * 1.15)

# Line on secondary axis
ax2 = ax1.twinx()
line = ax2.plot(
    x_pos,
    df['whiff_rate_per_swing'].values,
    marker='o',
    linewidth=2.5,
    color=palette_line,
    markersize=8,
    markeredgewidth=2,
    markeredgecolor='white'
)

ax2.set_ylabel("Whiff Rate per Swing", color=palette_line, fontweight='medium')

# Clean spines
for ax in [ax1, ax2]:
    ax.spines['top'].set_visible(False)
ax1.spines['right'].set_visible(False)

# Single combined legend
from matplotlib.lines import Line2D
legend_elements = [
    plt.Rectangle((0,0),1,1, fc=palette_bar, alpha=0.8, label='Avg xwOBA on BIP'),
    Line2D([0], [0], color=palette_line, linewidth=2.5, marker='o', 
           label='Whiff Rate per Swing'),
    Line2D([0], [0], color='gray', linestyle=':', linewidth=1.5, 
           label=f'Baseline xwOBA: {baseline:.3f}')
]
ax1.legend(handles=legend_elements, loc='upper left', frameon=True, 
          fancybox=False, shadow=False)

sns.despine(left=False, right=False)
plt.tight_layout()

def main():
    plt.show()

if __name__ == "__main__":
    main()