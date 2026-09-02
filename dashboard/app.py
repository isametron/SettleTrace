"""SettleTrace dashboard — the presentation layer over the audit trail.

Reads the audit log produced by the pipeline (either the committed local run in
`data/audit_log/`, or the live `audit_log` Delta table on Databricks) and shows
what a reconciliation lead would actually want: how much money cleared, how much
is held for review, and — for every held order — what the deterministic engine
found and what the advisory agent thinks about it.

The two things this deliberately puts on screen, because they are the point of
the project rather than decoration:

- **Autonomous actions taken: 0.** Stated as a headline number, not a footnote.
  The system explains and recommends; a human acts.
- **Engine vs. agent, side by side.** Where they disagree the card says so
  instead of silently picking a winner.

Run:  .venv\\Scripts\\streamlit.exe run dashboard/app.py
"""

import html
import json
from pathlib import Path

import pandas as pd
import plotly.graph_objects as go
import streamlit as st

REPO_ROOT = Path(__file__).resolve().parent.parent
LOCAL_AUDIT_LOG = REPO_ROOT / "data" / "audit_log" / "audit_log.jsonl"
LOCAL_RUN_SUMMARY = REPO_ROOT / "data" / "audit_log" / "run_summary.json"
DEFAULT_WAREHOUSE_ID = "ec6d536f6e40922e"

# --- Palette -----------------------------------------------------------------
# Light steps from the validated reference palette, against the light chart
# surface. Categorical slots are used in fixed order and never cycled; the
# status slots are reserved for state and never stand in for a series.
SURFACE = "#ffffff"
PAGE = "#f4f4f2"
INK = "#0b0b0b"
INK_SECONDARY = "#52514e"
INK_MUTED = "#898781"
GRIDLINE = "#e1e0d9"
BASELINE = "#c3c2b7"
HAIRLINE = "rgba(11,11,11,0.10)"

SERIES_1 = "#2a78d6"  # blue
SERIES_2 = "#eb6834"  # orange
SERIES_3 = "#1baf7a"  # aqua

# The headline figures take the sequential blue, as in the reference dashboards.
HERO = "#2a78d6"

STATUS_GOOD = "#0ca30c"
STATUS_WARNING = "#fab219"
STATUS_SERIOUS = "#ec835a"
STATUS_CRITICAL = "#d03b3b"

# Category → status role. These are genuine states (how bad is this exception),
# not series identity, so they take status tokens — always paired with the
# category label, never carrying meaning by hue alone.
CATEGORY_STATUS = {
    "clean_match": STATUS_GOOD,
    "timing_lag_refund": STATUS_WARNING,
    "mdr_rate_mismatch": STATUS_SERIOUS,
    "duplicate_transaction": STATUS_CRITICAL,
    "missing_payout": STATUS_CRITICAL,
}
CATEGORY_LABEL = {
    "clean_match": "Clean match",
    "timing_lag_refund": "Timing lag — refund",
    "mdr_rate_mismatch": "MDR rate mismatch",
    "duplicate_transaction": "Duplicate transaction",
    "missing_payout": "Missing payout",
}
TIER_LABEL = {"exact": "Exact", "fuzzy": "Fuzzy", "no_match": "No match"}

FONT = 'system-ui, -apple-system, "Segoe UI", sans-serif'


# --- Formatting ---------------------------------------------------------------
def inr(value: float, decimals: int = 2) -> str:
    """Indian digit grouping: ₹12,34,567.89 rather than ₹1,234,567.89."""
    if value is None:
        return "—"
    negative = value < 0
    whole, _, frac = f"{abs(value):.{decimals}f}".partition(".")
    if len(whole) > 3:
        head, tail = whole[:-3], whole[-3:]
        parts = []
        while len(head) > 2:
            parts.insert(0, head[-2:])
            head = head[:-2]
        if head:
            parts.insert(0, head)
        whole = ",".join([*parts, tail])
    body = f"{whole}.{frac}" if decimals else whole
    return f"{'−' if negative else ''}₹{body}"


def quote_html(text: str) -> str:
    """Escape model-written text and keep its line breaks inside the quote block.

    Two reasons this exists rather than passing the string straight through:

    - **It is model output.** Rendering it with `unsafe_allow_html` would let
      anything the model emitted — or anything an upstream field smuggled into
      the prompt — execute as markup in the reviewer's browser. Escape first.
    - Streamlit would otherwise parse the model's `- ` lines as a markdown list
      and hoist them out of the styled quote, so the agent column loses the rule
      the engine column has.
    """
    if not text or (isinstance(text, float) and pd.isna(text)):
        return ""
    lines = []
    for raw in html.escape(str(text)).splitlines():
        line = raw.strip()
        if not line:
            continue
        lines.append(f"• {line[2:]}" if line.startswith("- ") else line)
    return "<br>".join(lines)


def inr_compact(value: float) -> str:
    """Lakh/crore short form for headline figures."""
    if value is None:
        return "—"
    if abs(value) >= 1e7:
        return f"₹{value / 1e7:,.2f} Cr"
    if abs(value) >= 1e5:
        return f"₹{value / 1e5:,.2f} L"
    return f"₹{value:,.0f}"


# --- Data ---------------------------------------------------------------------
@st.cache_data(show_spinner=False)
def load_local() -> tuple[pd.DataFrame, dict]:
    if not LOCAL_AUDIT_LOG.exists():
        raise FileNotFoundError(
            f"{LOCAL_AUDIT_LOG} not found — run `python scripts/run_pipeline.py` first."
        )
    records = [json.loads(line) for line in LOCAL_AUDIT_LOG.read_text(encoding="utf-8").splitlines() if line.strip()]
    flat = []
    for record in records:
        row = dict(record)
        evidence = row.pop("engine_evidence", {}) or {}
        row.update({f"evidence_{k}": v for k, v in evidence.items()})
        flat.append(row)
    summary = {}
    if LOCAL_RUN_SUMMARY.exists():
        summary = json.loads(LOCAL_RUN_SUMMARY.read_text(encoding="utf-8"))
    return pd.DataFrame(flat), summary


@st.cache_data(show_spinner="Querying Databricks…")
def load_databricks(catalog: str, schema: str, warehouse_id: str) -> tuple[pd.DataFrame, dict]:
    from databricks.sdk import WorkspaceClient

    client = WorkspaceClient()
    response = client.statement_execution.execute_statement(
        statement=f"SELECT * FROM {catalog}.{schema}.audit_log",
        warehouse_id=warehouse_id,
        wait_timeout="50s",
    )
    state = response.status.state.value if response.status and response.status.state else "?"
    if state != "SUCCEEDED":
        raise RuntimeError(f"Databricks query {state}: {response.status.error if response.status else ''}")

    columns = [c.name for c in response.manifest.schema.columns]
    rows = response.result.data_array or []
    frame = pd.DataFrame(rows, columns=columns)

    numeric = [c for c in frame.columns if c.startswith("evidence_") or c in ("agent_confidence", "agent_latency_ms")]
    for column in numeric:
        frame[column] = pd.to_numeric(frame[column], errors="coerce")
    for column in ("agent_invoked", "autonomous_action_taken"):
        if column in frame.columns:
            frame[column] = frame[column].astype(str).str.lower().isin(("true", "1"))
    return frame, {}


# --- Chart helpers ------------------------------------------------------------
def base_layout(fig: go.Figure, height: int) -> go.Figure:
    fig.update_layout(
        height=height,
        paper_bgcolor=SURFACE,
        plot_bgcolor=SURFACE,
        font={"family": FONT, "color": INK_SECONDARY, "size": 13},
        margin={"l": 0, "r": 16, "t": 8, "b": 8},
        hoverlabel={"bgcolor": PAGE, "bordercolor": HAIRLINE, "font": {"family": FONT, "color": INK}},
        showlegend=False,
    )
    return fig


def value_split_chart(cleared: float, flagged: float) -> go.Figure:
    """Part-to-whole of settlement value: cleared vs held for review.

    One stacked horizontal bar rather than a two-slice pie — the reader's job is
    "how much of the money is held up", which a proportion bar answers directly.
    Both segments are direct-labelled, so the split is never tooltip-only.
    """
    total = cleared + flagged or 1
    fig = go.Figure()
    for label, value, color in (
        ("Cleared", cleared, SERIES_1),
        ("Held for review", flagged, SERIES_2),
    ):
        share = value / total * 100
        # Label the segment only where it comfortably fits; the held sliver is
        # ~8% wide, so its value is carried by the legend and tooltip instead of
        # a label cropped by its own segment.
        inside = f"{share:.1f}%" if share >= 15 else ""
        fig.add_bar(
            y=[""],
            x=[value],
            name=label,
            orientation="h",
            marker={"color": color, "line": {"width": 2, "color": SURFACE}},
            text=[inside],
            textposition="inside",
            insidetextanchor="middle",
            # White on the saturated fill — the only place white text is right
            # on a light surface, because it sits on the mark, not the page.
            textfont={"family": FONT, "color": "#ffffff", "size": 13},
            hovertemplate=f"<b>{label}</b><br>{inr(value)}<br>%{{customdata:.1f}}%<extra></extra>",
            customdata=[share],
        )
    fig.update_layout(barmode="stack", bargap=0.3)
    fig.update_xaxes(visible=False)
    fig.update_yaxes(visible=False)
    return base_layout(fig, 104)


def exceptions_by_category_chart(frame: pd.DataFrame) -> go.Figure:
    """Exception count by type — nominal categories, so one hue for every bar.

    Clean matches are excluded on purpose: at 136 of 150 they compress every
    exception bar to a few pixels, and the clean count is already the headline
    of the KPI row. This chart's job is the 14 orders that need a person.

    Colouring each bar darker-where-bigger would double-encode bar length as hue
    and burn the only free channel on information the bar already shows.
    """
    exceptions = frame[frame["engine_category"] != "clean_match"]
    grouped = (
        exceptions.groupby("engine_category")
        .agg(count=("order_id", "size"), value=("evidence_order_amount", "sum"))
        .reset_index()
        .sort_values("count")
    )
    labels = [CATEGORY_LABEL.get(c, c) for c in grouped["engine_category"]]

    fig = go.Figure(
        go.Bar(
            y=labels,
            x=grouped["count"],
            orientation="h",
            marker={"color": SERIES_1, "line": {"width": 2, "color": SURFACE}},
            text=[
                f"{n}  ·  {inr(v, decimals=0)}"
                for n, v in zip(grouped["count"], grouped["value"], strict=True)
            ],
            textposition="outside",
            cliponaxis=False,
            textfont={"family": FONT, "color": INK_SECONDARY, "size": 12},
            hovertemplate="<b>%{y}</b><br>%{x} orders held<extra></extra>",
        )
    )
    # Headroom for the outside labels — without it the longest bar's label is
    # clipped by the plot edge.
    fig.update_xaxes(visible=False, range=[0, max(grouped["count"].max() * 2.2, 1)])
    fig.update_yaxes(
        showgrid=False,
        zeroline=False,
        linecolor=BASELINE,
        tickfont={"family": FONT, "color": INK_SECONDARY, "size": 13},
    )
    fig.update_layout(bargap=0.45, margin={"l": 0, "r": 44, "t": 4, "b": 4})
    return base_layout(fig, max(46 * len(grouped) + 24, 140))


# --- Page ---------------------------------------------------------------------
st.set_page_config(
    page_title="SettleTrace — Settlement Reconciliation",
    page_icon="🧾",
    layout="wide",
    initial_sidebar_state="expanded",
)

st.markdown(
    f"""
    <style>
      .stApp {{ background: {PAGE}; }}
      .block-container {{ padding-top: 2.2rem; padding-bottom: 3rem; max-width: 1500px; }}
      #MainMenu, footer, header {{ visibility: hidden; }}

      .st-title {{ font-family: {FONT}; }}
      .brand {{
        font-family: {FONT}; font-size: 1.6rem; font-weight: 650;
        color: {INK}; letter-spacing: -0.015em; margin: 0;
      }}
      .brand-sub {{
        font-family: {FONT}; font-size: 0.86rem; color: {INK_MUTED};
        margin: 0.15rem 0 0 0;
      }}
      .prov {{
        font-family: {FONT}; font-size: 0.76rem; color: {INK_MUTED};
        text-align: right; line-height: 1.65;
      }}
      .prov code {{
        background: {PAGE}; border: 1px solid {HAIRLINE}; border-radius: 4px;
        padding: 1px 6px; color: {INK_SECONDARY}; font-size: 0.72rem;
      }}

      /* Headline figures, in the manner of the reference BI dashboards: the
         label sits above in plain sentence case, the number carries the weight
         and the colour, and the whole block breathes. */
      .tile {{
        background: {SURFACE}; border: 1px solid {HAIRLINE}; border-radius: 8px;
        padding: 1.35rem 1.25rem 1.15rem; height: 100%;
        display: flex; flex-direction: column; text-align: center;
      }}
      .tile-label {{
        font-family: {FONT}; font-size: 0.95rem; font-weight: 400;
        color: {INK_SECONDARY}; min-height: 2.8em; line-height: 1.35;
      }}
      /* Sized so the widest figure the data produces (a full rupee amount like
         ₹33,240) still fits one line inside a fifth of the row. */
      .tile-value {{
        font-family: {FONT}; font-size: 2.5rem; font-weight: 300; color: {HERO};
        line-height: 1.05; letter-spacing: -0.02em;
        white-space: nowrap;   /* never break a figure mid-digits */
      }}
      .tile-value.sm {{ font-size: 1.9rem; }}
      .tile-note {{
        font-family: {FONT}; font-size: 0.78rem; color: {INK_MUTED};
        margin-top: 0.6rem; line-height: 1.45;
      }}
      .tile.accent .tile-value {{ color: {STATUS_GOOD}; }}

      /* Streamlit renders each element in its own block, so a hand-written
         <div class="card"> can't wrap a chart. Style the native bordered
         container instead — st.container(border=True). */
      div[data-testid="stVerticalBlockBorderWrapper"] {{
        background: {SURFACE}; border: 1px solid {HAIRLINE};
        border-radius: 10px; padding: 1.1rem 1.25rem;
      }}
      div[data-testid="stExpander"] details {{
        background: {SURFACE}; border: 1px solid {HAIRLINE}; border-radius: 10px;
      }}
      div[data-testid="stExpander"] summary {{ font-family: {FONT}; font-size: 0.9rem; }}
      /* Section headings in the reference style: sentence case, dark, with the
         qualifier carried by a lighter sub-label rather than more emphasis. */
      .card-title {{
        font-family: {FONT}; font-size: 1rem; font-weight: 600;
        color: {INK}; margin-bottom: 0.15rem;
      }}
      .card-sub {{
        font-family: {FONT}; font-size: 0.82rem; color: {INK_MUTED};
        margin-bottom: 0.9rem;
      }}
      /* Small caps headings used inside a card (the two columns of an exception
         card) and in the sidebar — distinct from the section headings above. */
      .col-title {{
        font-family: {FONT}; font-size: 0.7rem; font-weight: 700;
        letter-spacing: 0.08em; text-transform: uppercase; color: {INK_MUTED};
        margin-bottom: 0.6rem;
      }}
      .side-label {{
        font-family: {FONT}; font-size: 0.7rem; font-weight: 700;
        letter-spacing: 0.08em; text-transform: uppercase; color: {INK_MUTED};
        margin-bottom: 0.6rem;
      }}

      .chip {{
        display: inline-block; font-family: {FONT}; font-size: 0.72rem;
        font-weight: 600; padding: 2px 9px; border-radius: 999px;
        border: 1px solid {HAIRLINE}; color: {INK_SECONDARY}; margin-right: 6px;
      }}
      .legend-dot {{
        display: inline-block; width: 9px; height: 9px; border-radius: 2px;
        margin-right: 7px; vertical-align: middle;
      }}
      .legend-item {{
        display: inline-block; font-family: {FONT}; font-size: 0.8rem;
        color: {INK_SECONDARY}; margin-right: 20px;
      }}
      .mono {{
        font-family: ui-monospace, "Cascadia Code", Consolas, monospace;
        font-size: 0.78rem; color: {INK_MUTED};
      }}
      .kv {{ font-family: {FONT}; font-size: 0.84rem; color: {INK_SECONDARY}; line-height: 1.9; }}
      .kv b {{ color: {INK}; font-variant-numeric: tabular-nums; }}
      .kv .bad {{ color: {STATUS_CRITICAL}; font-variant-numeric: tabular-nums; font-weight: 600; }}
      .quote {{
        font-family: {FONT}; font-size: 0.88rem; color: {INK_SECONDARY};
        line-height: 1.6; border-left: 2px solid {BASELINE}; padding-left: 0.85rem;
      }}
      .noact {{
        font-family: {FONT}; font-size: 0.74rem; color: {INK_MUTED};
        border-top: 1px solid {HAIRLINE}; padding-top: 0.6rem; margin-top: 0.85rem;
      }}
      section[data-testid="stSidebar"] {{ background: {SURFACE}; border-right: 1px solid {HAIRLINE}; }}
    </style>
    """,
    unsafe_allow_html=True,
)

# --- Sidebar: source + filters (one panel scoping the whole page) -------------
with st.sidebar:
    st.markdown("<div class='side-label'>Data source</div>", unsafe_allow_html=True)
    source = st.radio(
        "Data source",
        ["Local run (fast)", "Databricks (live)"],
        label_visibility="collapsed",
        help="Local reads the committed audit log. Databricks queries the audit_log Delta table.",
    )
    catalog = schema = warehouse = None
    if source.startswith("Databricks"):
        catalog = st.text_input("Catalog", "workspace")
        schema = st.text_input("Schema", "settletrace")
        warehouse = st.text_input("Warehouse ID", DEFAULT_WAREHOUSE_ID)
        st.caption("A stopped warehouse takes ~30s to wake.")

try:
    if source.startswith("Databricks"):
        df, run_summary = load_databricks(catalog, schema, warehouse)
    else:
        df, run_summary = load_local()
except Exception as exc:  # noqa: BLE001 -- surface any load failure in the UI
    st.error(f"Could not load the audit log: {exc}")
    st.stop()

for column in ("evidence_order_amount", "agent_confidence"):
    if column in df.columns:
        df[column] = pd.to_numeric(df[column], errors="coerce")

with st.sidebar:
    st.divider()
    st.markdown("<div class='side-label'>Filters</div>", unsafe_allow_html=True)

    categories = sorted(df["engine_category"].unique())
    picked_categories = st.multiselect(
        "Exception category",
        categories,
        default=categories,
        format_func=lambda c: CATEGORY_LABEL.get(c, c),
    )
    tiers = sorted(df["engine_match_tier"].unique())
    picked_tiers = st.multiselect(
        "Match tier", tiers, default=tiers, format_func=lambda t: TIER_LABEL.get(t, t)
    )
    review_options = sorted(df["review_status"].unique())
    picked_review = st.multiselect(
        "Review status",
        review_options,
        default=review_options,
        format_func=lambda s: s.replace("_", " ").capitalize(),
    )
    amounts = df["evidence_order_amount"].dropna()
    min_amount = st.slider(
        "Minimum order value (₹)",
        0,
        int(amounts.max()) + 1 if len(amounts) else 1,
        0,
        step=50,
    )

mask = (
    df["engine_category"].isin(picked_categories)
    & df["engine_match_tier"].isin(picked_tiers)
    & df["review_status"].isin(picked_review)
    & (df["evidence_order_amount"].fillna(0) >= min_amount)
)
view = df[mask]

# --- Header -------------------------------------------------------------------
head_left, head_right = st.columns([3, 2])
with head_left:
    st.markdown(
        "<p class='brand'>SettleTrace</p>"
        "<p class='brand-sub'>Settlement reconciliation — matched, explained, and held for review</p>",
        unsafe_allow_html=True,
    )
with head_right:
    model = next((m for m in df.get("agent_model", pd.Series(dtype=str)).dropna().unique()), "—")
    prompt_version = next(
        (p for p in df.get("agent_prompt_version", pd.Series(dtype=str)).dropna().unique()), "—"
    )
    run_id = df["run_id"].iloc[0] if len(df) else "—"
    started = run_summary.get("run_started_at") or (
        df["run_started_at"].iloc[0] if "run_started_at" in df.columns and len(df) else "—"
    )
    st.markdown(
        f"<div class='prov'>run <code>{run_id}</code> &nbsp; {started}<br>"
        f"model <code>{model}</code> &nbsp; prompt <code>{prompt_version}</code></div>",
        unsafe_allow_html=True,
    )

st.write("")

# --- KPI row ------------------------------------------------------------------
total_orders = len(view)
auto_cleared = int((view["review_status"] == "auto_cleared").sum())
needs_review = int((view["review_status"] == "needs_human_review").sum())
value_cleared = float(view.loc[view["review_status"] == "auto_cleared", "evidence_order_amount"].sum())
value_flagged = float(
    view.loc[view["review_status"] == "needs_human_review", "evidence_order_amount"].sum()
)
total_value = value_cleared + value_flagged
auto_rate = (auto_cleared / total_orders) if total_orders else 0.0

held_share = (value_flagged / total_value) if total_value else 0.0

k1, k2, k3, k4, k5 = st.columns(5)
tiles = [
    (k1, "Auto-cleared", f"{auto_rate:.1%}", f"{auto_cleared} of {total_orders} orders", False),
    (k2, "Value cleared", inr_compact(value_cleared), inr(value_cleared), False),
    (k3, "Value held", inr_compact(value_flagged), f"{held_share:.1%} of settled value", False),
    (k4, "Needs review", f"{needs_review}", "every flagged order, whatever the agent's confidence", False),
    (k5, "Autonomous actions", "0", "advisory only — no payout, ledger, or write-back", True),
]
for column, label, value, note, accent in tiles:
    with column:
        size_class = "" if len(value) <= 8 else " sm"
        accent_class = " accent" if accent else ""
        st.markdown(
            f"<div class='tile{accent_class}'>"
            f"<div class='tile-label'>{label}</div>"
            f"<div class='tile-value{size_class}'>{value}</div>"
            f"<div class='tile-note'>{note}</div></div>",
            unsafe_allow_html=True,
        )

st.write("")

# --- Charts -------------------------------------------------------------------
chart_left, chart_right = st.columns([1, 1])

with chart_left, st.container(border=True):
    st.markdown(
        "<div class='card-title'>Settlement value</div>"
        "<div class='card-sub'>Cleared vs held for review</div>",
        unsafe_allow_html=True,
    )
    if total_value:
        st.plotly_chart(
            value_split_chart(value_cleared, value_flagged),
            width="stretch",
            config={"displayModeBar": False},
        )
        # Legend is always present for two series, and the large segment is
        # direct-labelled, so the split is never reachable only via tooltip.
        st.markdown(
            f"<span class='legend-item'><span class='legend-dot' style='background:{SERIES_1}'></span>"
            f"Cleared &nbsp;<b style='color:{INK}'>{inr(value_cleared)}</b></span>"
            f"<span class='legend-item'><span class='legend-dot' style='background:{SERIES_2}'></span>"
            f"Held for review &nbsp;<b style='color:{INK}'>{inr(value_flagged)}</b></span>",
            unsafe_allow_html=True,
        )
    else:
        st.caption("No orders match the current filters.")

with chart_right, st.container(border=True):
    st.markdown(
        "<div class='card-title'>Exceptions by type</div>"
        "<div class='card-sub'>Order count and value held</div>",
        unsafe_allow_html=True,
    )
    if len(view[view["engine_category"] != "clean_match"]):
        st.plotly_chart(
            exceptions_by_category_chart(view),
            use_container_width=True,
            config={"displayModeBar": False},
        )
    else:
        st.caption("No exceptions match the current filters.")

# --- Exception queue ----------------------------------------------------------
queue = view[view["review_status"] == "needs_human_review"].copy()
queue = queue.sort_values("evidence_order_amount", ascending=False)

st.markdown(
    f"<div class='card-title' style='margin-top:1.6rem'>Exception queue</div>"
    f"<div class='card-sub'>{len(queue)} orders held &nbsp;·&nbsp; {inr(value_flagged)} "
    f"&nbsp;·&nbsp; each awaiting a human decision</div>",
    unsafe_allow_html=True,
)

if not len(queue):
    st.info("Nothing held for review under the current filters.")

for _, row in queue.iterrows():
    category = row["engine_category"]
    colour = CATEGORY_STATUS.get(category, STATUS_WARNING)
    agreement = row.get("engine_agent_agreement", "not_assessed")
    agrees = agreement == "agree"
    agreement_chip = {
        "agree": f"<span class='chip' style='border-color:{STATUS_GOOD};color:{STATUS_GOOD}'>✓ Agent agrees</span>",
        "disagree": f"<span class='chip' style='border-color:{STATUS_CRITICAL};color:{STATUS_CRITICAL}'>⚠ Agent disagrees</span>",
        "not_assessed": "<span class='chip'>Not reviewed by agent</span>",
    }.get(agreement, "")

    title = (
        f"{CATEGORY_LABEL.get(category, category)}  ·  {inr(row['evidence_order_amount'])}"
        f"  ·  {row['order_id'][:8]}"
    )
    with st.expander(title, expanded=False):
        st.markdown(
            f"<span class='chip' style='border-color:{colour};color:{colour}'>"
            f"{CATEGORY_LABEL.get(category, category)}</span>"
            f"<span class='chip'>{TIER_LABEL.get(row['engine_match_tier'], row['engine_match_tier'])}</span>"
            f"{agreement_chip}"
            f"<span class='mono' style='float:right'>{row['order_id']}</span>",
            unsafe_allow_html=True,
        )
        st.write("")

        left, right = st.columns([1, 1])
        with left:
            st.markdown(
                "<div class='col-title'>Rule engine — authoritative</div>",
                unsafe_allow_html=True,
            )
            st.markdown(
                f"<div class='quote'>{quote_html(row['engine_reasoning'])}</div>",
                unsafe_allow_html=True,
            )
            st.write("")

            def cell(label, expected, actual):
                if expected is None or actual is None or pd.isna(expected) or pd.isna(actual):
                    return ""
                differs = abs(float(expected) - float(actual)) > 0.01
                actual_class = "bad" if differs else ""
                return (
                    f"{label} &nbsp; expected <b>{inr(expected)}</b> &nbsp;·&nbsp; "
                    f"actual <span class='{actual_class}'>{inr(actual)}</span><br>"
                )

            evidence = (
                cell("MDR fee", row.get("evidence_expected_mdr_fee"), row.get("evidence_actual_mdr_fee"))
                + cell(
                    "Refund adj.",
                    row.get("evidence_expected_refund_adjustment"),
                    row.get("evidence_actual_refund_adjustment"),
                )
                + cell(
                    "Net amount",
                    row.get("evidence_expected_net_amount"),
                    row.get("evidence_actual_net_amount"),
                )
                + f"Settlement lines &nbsp; <b>{int(row['evidence_settlement_row_count'])}</b><br>"
                + f"Batch &nbsp; <span class='mono'>{row.get('evidence_settlement_batch_id', '—')}</span>"
            )
            st.markdown(f"<div class='kv'>{evidence}</div>", unsafe_allow_html=True)

        with right:
            confidence = row.get("agent_confidence")
            header = "Agent — advisory"
            if pd.notna(confidence):
                header += f" &nbsp;·&nbsp; confidence {float(confidence):.2f}"
            st.markdown(f"<div class='col-title'>{header}</div>", unsafe_allow_html=True)

            if pd.isna(row.get("agent_cause")) or not row.get("agent_cause"):
                st.markdown(
                    f"<div class='kv' style='color:{INK_MUTED}'>"
                    f"{quote_html(row.get('agent_skip_reason')) or 'Not diagnosed.'}</div>",
                    unsafe_allow_html=True,
                )
            else:
                st.markdown(
                    f"<div class='quote'>{quote_html(row['agent_explanation'])}</div>",
                    unsafe_allow_html=True,
                )
                st.write("")
                diagnosis_label = CATEGORY_LABEL.get(row["agent_cause"], row["agent_cause"])
                st.markdown(
                    f"<div class='kv'>Diagnosis &nbsp; <b>{html.escape(str(diagnosis_label))}</b><br>"
                    f"Recommended &nbsp; {quote_html(row['agent_recommended_action'])}</div>",
                    unsafe_allow_html=True,
                )
                if not agrees:
                    st.markdown(
                        f"<div class='kv' style='color:{STATUS_CRITICAL};margin-top:0.5rem'>"
                        f"Engine and agent differ — escalated rather than resolved toward either.</div>",
                        unsafe_allow_html=True,
                    )

        st.markdown(
            f"<div class='noact'>Action taken: <b style='color:{INK_SECONDARY}'>none</b> &nbsp;·&nbsp; "
            f"autonomous_action_taken = false &nbsp;·&nbsp; "
            f"{quote_html(row.get('review_reason', ''))}</div>",
            unsafe_allow_html=True,
        )

# --- Table view (the WCAG-clean twin of every chart above) --------------------
with st.expander(f"Table view — all {len(view)} orders in scope"):
    columns = [
        "order_id",
        "engine_match_tier",
        "engine_category",
        "evidence_order_amount",
        "agent_cause",
        "agent_confidence",
        "engine_agent_agreement",
        "review_status",
        "action_taken",
        "autonomous_action_taken",
    ]
    st.dataframe(
        view[[c for c in columns if c in view.columns]],
        width="stretch",
        hide_index=True,
    )
    st.download_button(
        "Download this slice as CSV",
        view.to_csv(index=False).encode("utf-8"),
        file_name="settletrace_audit_slice.csv",
        mime="text/csv",
    )
