import os, re, sys
import pandas as pd

INPUT_PATH = r"E:\KULIAH FIRA\Kerja Praktik\New folder\Data_2022_fiks.xls"
OUT_DIRNAME = "REKAP_HASIL"
OUT_XLSX = "rekap_FINAL_2022_fiks.xlsx"

def read_any_excel(path: str) -> pd.DataFrame:
    ext = os.path.splitext(path)[1].lower()
    engine = "xlrd" if ext == ".xls" else "openpyxl"
    try:
        df = pd.read_excel(
            path, engine=engine, header=None,
            names=["Tanggal", "Nama", "Jumlah", "Kode", "Wilayah"]
        )
        return df
    except Exception as e:
        print(f"[ERROR] Gagal membaca {path} (engine={engine}): {e}")
        sys.exit(1)

def normalize_name(raw: str) -> str:
    if not raw or str(raw).strip() == "":
        return ""
    s = str(raw).strip().replace("\\", "/")
    if "/" in s:
        s = s.split("/", 1)[0].strip()
    s = re.sub(r"\s+0*\d+$", "", s).strip()
    s = re.sub(r"\s{2,}", " ", s).strip()
    return s.upper()

# Ambil wilayah paling sering (mode) versi kapital
def most_frequent(series: pd.Series) -> str:
    if series.empty:
        return ""
    counts = series.str.upper().value_counts(dropna=True)
    top = counts.max()
    candidates = [v for v, c in counts.items() if c == top and v.strip() != ""]
    candidates.sort(key=lambda x: (len(x), x))
    return candidates[0] if candidates else ""

def main():
    if not os.path.exists(INPUT_PATH):
        print(f"[ERROR] File tidak ditemukan: {INPUT_PATH}")
        sys.exit(1)

    print("[INFO] Membaca Excel ...")
    df = read_any_excel(INPUT_PATH)
    print(f"[INFO] Data terbaca: {df.shape[0]} baris, {df.shape[1]} kolom")

    df["Nama"]    = df["Nama"].astype(str).str.strip()
    df["Wilayah"] = df["Wilayah"].astype(str).str.strip().str.upper()
    df["Jumlah"]  = pd.to_numeric(df["Jumlah"], errors="coerce")
    df["Nama_Final"] = df["Nama"].apply(normalize_name)

    df_valid = df.loc[
        (df["Nama_Final"].str.len() > 0) &
        (df["Jumlah"].notna()) &
        (df["Jumlah"] > 0)
    ].copy()

    # === SHEET 1: REKAP PER NAMA (wilayah = mode) ===
    agg = (
        df_valid.groupby("Nama_Final", as_index=False)
        .agg(Total_Jumlah=("Jumlah", "sum"),
             Total_Transaksi=("Jumlah", "size"))
    )
    wilayah_mode = (
        df_valid.groupby("Nama_Final")["Wilayah"]
        .apply(most_frequent)
        .reset_index(name="Wilayah")
    )
    rekap_per_nama = (
        agg.merge(wilayah_mode, on="Nama_Final", how="left")
           .rename(columns={"Nama_Final": "Nama"})
           .loc[:, ["Nama", "Wilayah", "Total_Jumlah", "Total_Transaksi"]]
           .sort_values("Total_Jumlah", ascending=False)
           .reset_index(drop=True)
    )

    # === SHEET 2: REKAP PER NAMA & WILAYAH (detail) ===
    rekap_per_nama_wilayah = (
        df_valid.groupby(["Nama_Final", "Wilayah"], as_index=False)
        .agg(Total_Jumlah=("Jumlah", "sum"),
             Total_Transaksi=("Jumlah", "size"))
        .rename(columns={"Nama_Final": "Nama"})
        .sort_values(["Nama", "Total_Jumlah"], ascending=[True, False])
        .reset_index(drop=True)
    )

    out_dir = os.path.join(os.path.dirname(INPUT_PATH), OUT_DIRNAME)
    os.makedirs(out_dir, exist_ok=True)
    xlsx_path = os.path.join(out_dir, OUT_XLSX)

    with pd.ExcelWriter(xlsx_path) as writer:
        rekap_per_nama.to_excel(writer, index=False, sheet_name="Per_Nama")
        rekap_per_nama_wilayah.to_excel(writer, index=False, sheet_name="Per_Nama_Wilayah")

    print("\n✅ Selesai!")
    print("📄 XLSX (2 sheet):", xlsx_path)

if __name__ == "__main__":
    main()