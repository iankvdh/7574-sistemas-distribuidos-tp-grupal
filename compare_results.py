#!/usr/bin/env python3
"""
compare_results.py — valida resultados del sistema distribuido contra referencia.

Modos:
    generate  Calcula las queries de referencia y las guarda en --ref-dir.
              Hacer una sola vez por dataset.

    compare   Compara TODOS los archivos *_qN.csv de --results-dir contra la
              referencia guardada. Reporta cada UUID encontrado.

Ejemplos:
    python compare_results.py generate --dataset small
    python compare_results.py generate --dataset medium --ref-dir reference/medium

    python compare_results.py compare
    python compare_results.py compare --results-dir results/client_1 --ref-dir reference/medium
    python compare_results.py compare --queries 4,5
"""

import argparse
import json
import sys
from pathlib import Path

import pandas as pd

ROOT = Path(__file__).parent

# ---------------------------------------------------------------------------
# Configuración de datasets
# ---------------------------------------------------------------------------

DATASETS = {
    "small": {
        "trans":    ROOT / "datasets/LI-Small_Trans.csv",
        "accounts": ROOT / "datasets/LI-Small_accounts.csv",
    },
    "medium": {
        "trans":    ROOT / "datasets/LI-Medium_Trans.csv",
        "accounts": ROOT / "datasets/LI-Medium_accounts.csv",
    },
}

COTIZACIONES = ROOT / "datasets/cotizaciones.json"

# ---------------------------------------------------------------------------
# Queries de referencia
# ---------------------------------------------------------------------------

def ref_q1(trans_df: pd.DataFrame) -> pd.DataFrame:
    usd = trans_df[trans_df["Payment Currency"] == "US Dollar"]
    result = usd[usd["Amount Paid"] < 50][["From Bank", "Account", "To Bank", "Account.1", "Amount Paid"]].copy()
    result["Amount Paid"] = result["Amount Paid"].round(2)
    return result.reset_index(drop=True)


def ref_q2(trans_df: pd.DataFrame, accounts_df: pd.DataFrame) -> pd.DataFrame:
    usd = trans_df[trans_df["Payment Currency"] == "US Dollar"]
    idx = usd.groupby("From Bank")["Amount Paid"].idxmax()
    top = usd.loc[idx]
    bank_names = accounts_df[["Bank ID", "Bank Name"]].drop_duplicates(subset=["Bank ID"])
    merged = top.merge(bank_names, left_on="From Bank", right_on="Bank ID", how="left")
    merged["Bank Name"] = merged.apply(
        lambda r: r["Bank Name"] if pd.notna(r["Bank Name"]) and str(r["Bank Name"]).strip() != ""
                  else f"UNKNOWN_{int(r['From Bank'])}",
        axis=1,
    )
    result = merged[["From Bank", "Account", "Bank Name", "Amount Paid"]].copy()
    result = result.rename(columns={"From Bank": "Bank ID"})
    result["Amount Paid"] = result["Amount Paid"].round(2)
    return result.reset_index(drop=True)


def ref_q3(trans_df: pd.DataFrame) -> pd.DataFrame:
    usd = trans_df[trans_df["Payment Currency"] == "US Dollar"]
    base  = usd[(usd["Timestamp"] >= "2022/09/01") & (usd["Timestamp"] < "2022/09/06")]
    eval_ = usd[(usd["Timestamp"] >= "2022/09/06") & (usd["Timestamp"] < "2022/09/16")]
    avg = (
        base.groupby("Payment Format", as_index=False)["Amount Paid"]
        .mean()
        .rename(columns={"Amount Paid": "AVG"})
    )
    merged = eval_.merge(avg, on="Payment Format")
    result = merged[merged["Amount Paid"] < merged["AVG"] * 0.01][["From Bank", "Account", "Amount Paid"]].copy()
    result["Amount Paid"] = result["Amount Paid"].round(2)
    return result.reset_index(drop=True)


def ref_q4(trans_df: pd.DataFrame) -> pd.DataFrame:
    usd    = trans_df[trans_df["Payment Currency"] == "US Dollar"]
    window = usd[(usd["Timestamp"] >= "2022/09/01") & (usd["Timestamp"] < "2022/09/06")]
    edges  = window[["From Bank", "Account", "To Bank", "Account.1"]].drop_duplicates()

    fanout     = edges.groupby(["From Bank", "Account"]).size().reset_index(name="n")
    candidates = fanout[fanout["n"] >= 5][["From Bank", "Account"]]

    first_hop = edges.merge(candidates, on=["From Bank", "Account"]).rename(columns={
        "From Bank": "Source Bank", "Account":   "Source Account",
        "To Bank":   "Interm Bank", "Account.1": "Interm Account",
    })
    second_hop = edges.rename(columns={
        "From Bank": "Interm Bank", "Account":   "Interm Account",
        "To Bank":   "Final Bank",  "Account.1": "Final Account",
    })
    paths = first_hop.merge(second_hop, on=["Interm Bank", "Interm Account"])
    paths = paths[
        (paths["Source Bank"] != paths["Final Bank"]) | (paths["Source Account"] != paths["Final Account"])
    ].copy()
    paths["interm_key"] = paths["Interm Bank"].astype(str) + "_" + paths["Interm Account"]

    counts = (
        paths.groupby(["Source Bank", "Source Account", "Final Bank", "Final Account"])["interm_key"]
        .nunique()
        .reset_index(name="n_intermediaries")
    )
    pairs = counts[counts["n_intermediaries"] >= 5][
        ["Source Bank", "Source Account", "Final Bank", "Final Account"]
    ]
    return pairs.rename(columns={
        "Source Bank": "From Bank", "Source Account": "Account",
        "Final Bank":  "To Bank",   "Final Account":  "Account.1",
    }).reset_index(drop=True)


def _build_rates_df(date_range: pd.DatetimeIndex) -> pd.DataFrame:
    CURRENCY_NAME_TO_CODE = {
        "Australian Dollar": "AUD", "Brazil Real": "BRL", "Canadian Dollar": "CAD",
        "Euro": "EUR", "Mexican Peso": "MXN", "Rupee": "INR", "Ruble": "RUB",
        "Saudi Riyal": "SAR", "Shekel": "ILS", "Swiss Franc": "CHF",
        "UK Pound": "GBP", "US Dollar": None, "Yen": "JPY", "Yuan": "CNY",
    }
    with open(COTIZACIONES) as f:
        entries = json.load(f)
    raw: dict = {}
    for e in entries:
        raw.setdefault(e["date"], {})[e["quote"]] = e["rate"]
    available = sorted(raw.keys())

    def rates_for(date_str: str) -> dict:
        candidates = [d for d in available if d <= date_str]
        return raw[candidates[-1] if candidates else available[0]]

    rows = []
    for dt in date_range:
        date_str = dt.strftime("%Y-%m-%d")
        day = rates_for(date_str)
        for name, code in CURRENCY_NAME_TO_CODE.items():
            usd_rate = 1.0 if code is None else 1.0 / day[code]
            rows.append({"Date": date_str, "Payment Currency": name, "usd_rate": usd_rate})
    return pd.DataFrame(rows)


def ref_q5(trans_df: pd.DataFrame) -> int:
    window   = trans_df[(trans_df["Timestamp"] >= "2022/09/01") & (trans_df["Timestamp"] < "2022/09/06")]
    wire_ach = window[window["Payment Format"].isin(["Wire", "ACH"])].copy()
    wire_ach["Date"] = pd.to_datetime(wire_ach["Timestamp"]).dt.strftime("%Y-%m-%d")
    rates_df  = _build_rates_df(pd.date_range("2022-09-01", "2022-09-05", freq="D"))
    wire_ach  = wire_ach.merge(rates_df, on=["Date", "Payment Currency"], how="left")
    wire_ach["Amount"] = wire_ach["Amount Paid"] * wire_ach["usd_rate"]
    return int(wire_ach[wire_ach["Amount"] < 1].shape[0])


# ---------------------------------------------------------------------------
# Subcomandos
# ---------------------------------------------------------------------------

def cmd_generate(args):
    ds = DATASETS[args.dataset]
    ref_dir = Path(args.ref_dir or f"notebooks/{args.dataset}/reference")
    ref_dir.mkdir(parents=True, exist_ok=True)

    print(f"Cargando {ds['trans'].name} ...")
    trans_df    = pd.read_csv(ds["trans"])
    accounts_df = pd.read_csv(ds["accounts"])

    queries = [int(q) for q in args.queries.split(",")]

    for q in queries:
        print(f"  Calculando Q{q} ...", end=" ", flush=True)
        if q == 1:
            ref_q1(trans_df).to_csv(ref_dir / "q1.csv", index=False)
        elif q == 2:
            ref_q2(trans_df, accounts_df).to_csv(ref_dir / "q2.csv", index=False)
        elif q == 3:
            ref_q3(trans_df).to_csv(ref_dir / "q3.csv", index=False)
        elif q == 4:
            ref_q4(trans_df).to_csv(ref_dir / "q4.csv", index=False)
        elif q == 5:
            count = ref_q5(trans_df)
            pd.DataFrame({"count": [count]}).to_csv(ref_dir / "q5.csv", index=False)
        print("OK")

    print(f"\nReferencia guardada en {ref_dir}/")


def _read_dist_result(path: Path, query: int) -> pd.DataFrame:
    if query == 3:
        # El CSV de Q3 tiene 4 columnas en el header pero solo 3 en los datos
        return pd.read_csv(path, names=["From Bank", "Account", "Amount Paid"], skiprows=1)
    return pd.read_csv(path)


def _sort(df: pd.DataFrame) -> pd.DataFrame:
    return df.astype(str).sort_values(by=list(df.columns)).reset_index(drop=True)


def _compare_q(query: int, ref_dir: Path, dist_path: Path) -> bool:
    ref_path = ref_dir / f"q{query}.csv"
    if not ref_path.exists():
        print(f"    [SKIP] Q{query}: no hay referencia en {ref_path}")
        return True

    ref  = _sort(pd.read_csv(ref_path))
    dist = _sort(_read_dist_result(dist_path, query))

    if query == 5:
        r = int(ref["count"].iloc[0])
        d = int(dist["count"].iloc[0])
        if r == d:
            print(f"    [OK]   Q{query}: {r}")
            return True
        print(f"    [FAIL] Q{query}: ref={r}, dist={d}")
        return False

    if ref.shape == dist.shape and ref.equals(dist):
        print(f"    [OK]   Q{query}: {len(ref)} filas")
        return True

    if ref.shape != dist.shape:
        print(f"    [FAIL] Q{query}: shapes distintos — ref={ref.shape}, dist={dist.shape}")
    else:
        diff = ref.compare(dist)
        print(f"    [FAIL] Q{query}: {len(ref)} filas, {len(diff)} diferencias")
        print(diff.head(5).to_string(index=False))
    return False


def cmd_compare(args):
    results_dir = Path(args.results_dir or f"results/{args.dataset}/client_0")
    ref_dir     = Path(args.ref_dir or f"notebooks/{args.dataset}/reference")
    queries     = [int(q) for q in args.queries.split(",")]

    if not results_dir.exists():
        print(f"Error: {results_dir} no existe", file=sys.stderr)
        sys.exit(1)
    if not ref_dir.exists():
        print(f"Error: referencia no encontrada en {ref_dir} — ejecutá primero: make generate-ref", file=sys.stderr)
        sys.exit(1)

    # Agrupar archivos de resultados por UUID
    uuid_files: dict[str, dict[int, Path]] = {}
    for f in results_dir.glob("*_q*.csv"):
        parts = f.stem.rsplit("_q", 1)
        if len(parts) == 2 and parts[1].isdigit():
            uuid, q = parts[0], int(parts[1])
            if q in queries:
                uuid_files.setdefault(uuid, {})[q] = f

    if not uuid_files:
        print(f"No se encontraron archivos *_qN.csv en {results_dir}")
        sys.exit(1)

    print(f"Referencia : {ref_dir}/")
    print(f"Resultados : {results_dir}/")
    print(f"Queries    : {queries}")
    print()

    total_uuids = 0
    passed_uuids = 0

    for uuid in sorted(uuid_files):
        print(f"  {uuid}")
        results = []
        for q in sorted(queries):
            if q not in uuid_files[uuid]:
                print(f"    [SKIP] Q{q}: archivo no encontrado")
                continue
            results.append(_compare_q(q, ref_dir, uuid_files[uuid][q]))

        total_uuids += 1
        if all(results):
            passed_uuids += 1

    print()
    print(f"Resultado: {passed_uuids}/{total_uuids} runs correctos")
    sys.exit(0 if passed_uuids == total_uuids else 1)


# ---------------------------------------------------------------------------
# CLI
# ---------------------------------------------------------------------------

def main():
    parser = argparse.ArgumentParser(description="Valida resultados del sistema distribuido")
    sub = parser.add_subparsers(dest="cmd", required=True)

    gen = sub.add_parser("generate", help="Genera CSVs de referencia desde el dataset")
    gen.add_argument("--dataset",  required=True, choices=list(DATASETS))
    gen.add_argument("--ref-dir",  default=None, help="Carpeta de salida (default: notebooks/<dataset>/reference)")
    gen.add_argument("--queries",  default="1,2,3,4,5")

    cmp = sub.add_parser("compare", help="Compara results/ contra la referencia")
    cmp.add_argument("--dataset",     default="small", choices=list(DATASETS),
                     help="Dataset de referencia a usar (default: small)")
    cmp.add_argument("--results-dir", default=None)
    cmp.add_argument("--ref-dir",     default=None, help="Carpeta de referencia (default: notebooks/<dataset>/reference)")
    cmp.add_argument("--queries",     default="1,2,3,4,5")

    args = parser.parse_args()
    if args.cmd == "generate":
        cmd_generate(args)
    else:
        cmd_compare(args)


if __name__ == "__main__":
    main()
