import os, re, sys
import pandas as pd

INPUT_PATH = r"E:\KULIAH FIRA\Kerja Praktik\New folder\Data 2025 - September.xls"
OUT_DIRNAME = "REKAP_HASIL"
OUT_XLSX   = "rekap_2025_September.xlsx"

def read_any_excel_smart(path: str) -> pd.DataFrame:
    ext = os.path.splitext(path)[1].lower()
    engine = "xlrd" if ext == ".xls" else "openpyxl"
    df = pd.read_excel(path, engine=engine)
    df.columns = [str(c).strip() for c in df.columns]
    return df

def normalize_name_keep(raw: str) -> str:
    if pd.isna(raw):
        return ""
    return str(raw).strip().upper()

def normalize_phone_0x(raw: str) -> str:
    if pd.isna(raw):
        return "" 
    s = re.sub(r"\D+", "", str(raw))
    if s == "":
        return ""
    if s.startswith("62"):
        s = "0" + s[2:] if len(s) > 2 else "0"
    elif s.startswith("8"):
        s = "0" + s
    elif s.startswith("0"):
        pass
    return s

def normalize_address(raw: str) -> str:
    if pd.isna(raw):
        return ""
    return str(raw).strip().upper()

def normalize_note(raw: str) -> str:
    if pd.isna(raw):
        return ""
    return str(raw).strip()

def most_frequent_str(series: pd.Series) -> str:
    s = series.dropna().astype(str).str.strip()
    if s.empty:
        return ""
    counts = s.value_counts()
    top = counts.max()
    candidates = [v for v, c in counts.items() if c == top and v != ""]
    candidates.sort(key=lambda x: (len(x), x))
    return candidates[0] if candidates else ""

def first_by_rowid(series_vals, series_rowid):
    idxmin = series_rowid.idxmin()
    return series_vals.loc[idxmin]

# ====== MAIN ======
def main():
    if not os.path.exists(INPUT_PATH):
        print(f"[ERROR] File tidak ditemukan: {INPUT_PATH}")
        sys.exit(1)

    print("[INFO] Membaca Excel ...")
    df = read_any_excel_smart(INPUT_PATH)

    # Pemetaan kolom fleksibel
    lower_cols = {c.lower(): c for c in df.columns}
    def pick(*cands):
        for nm in cands:
            if nm in lower_cols:
                return lower_cols[nm]
        return None

    col_nama   = pick("nama","name")
    col_hp     = pick("nomor hp","no hp","hp","telepon","phone")
    col_alamat = pick("alamat","address","lokasi")
    col_jml    = pick("jumlah ekor","jumlah","qty")
    col_ket    = pick("keterangan","ket","catatan","note","desc")

    work = pd.DataFrame({
        "Nama": df[col_nama] if col_nama in df.columns else "",
        "Nomor Hp": df[col_hp] if col_hp in df.columns else "",
        "Alamat": df[col_alamat] if col_alamat in df.columns else "",
        "Jumlah Ekor": df[col_jml] if col_jml in df.columns else 0,
        "Keterangan": df[col_ket] if col_ket in df.columns else ""
    }).copy()

    work.reset_index(drop=True, inplace=True)
    work["RowID"] = work.index

    # Normalisasi
    work["Nama_Final"]   = work["Nama"].apply(normalize_name_keep)
    work["HP_Final"]     = work["Nomor Hp"].apply(normalize_phone_0x)
    work["Alamat_Final"] = work["Alamat"].apply(normalize_address)
    work["Keterangan_Final"] = work["Keterangan"].apply(normalize_note)
    work["Jumlah Ekor"]  = pd.to_numeric(work["Jumlah Ekor"], errors="coerce").fillna(0)

    # ========= CLUSTERING ===============
    cluster_ids = []

    for i, row in work.iterrows():
        hp = row["HP_Final"]
        alamat = row["Alamat_Final"]
        nama = row["Nama_Final"]

        if hp and alamat:
            cluster_id = f"{hp}|{alamat}"  # utama: HP + alamat (untuk kasus EPU DONGGULU)
        elif hp:
            cluster_id = f"{nama}|P|{hp}"
        elif alamat:
            cluster_id = f"{nama}|A|{alamat}"
        else:
            cluster_id = f"{nama}|R|{i}"
        cluster_ids.append(cluster_id)

    work["cluster_id"] = cluster_ids

    # ======== AGREGASI PER CLUSTER ==========
    grouped = []
    for cid, grp in work.groupby("cluster_id", as_index=False):
        nama = first_by_rowid(grp["Nama_Final"], grp["RowID"])
        hp   = first_by_rowid(grp["HP_Final"], grp["RowID"])
        alamat = first_by_rowid(grp["Alamat_Final"], grp["RowID"])
        ket  = first_by_rowid(grp["Keterangan_Final"], grp["RowID"])
        jumlah = grp["Jumlah Ekor"].sum()
        transaksi = len(grp)
        grouped.append({
            "Nama": nama,
            "Nomor Hp": hp,
            "Alamat": alamat,
            "Keterangan": ket,
            "Jumlah Ekor": jumlah,
            "Jumlah Transaksi": transaksi,
            "RowID_min": grp["RowID"].min()
        })

    out = pd.DataFrame(grouped)
    out.sort_values(["Nama","Nomor Hp","Alamat","RowID_min"], inplace=True)
    out.drop(columns=["RowID_min"], inplace=True)
    out.reset_index(drop=True, inplace=True)
    out.insert(0, "No", range(1, len(out)+1))

    # ======== OUTPUT ==========
    out_dir = os.path.join(os.path.dirname(INPUT_PATH), OUT_DIRNAME)
    os.makedirs(out_dir, exist_ok=True)
    xlsx_path = os.path.join(out_dir, OUT_XLSX)

    with pd.ExcelWriter(xlsx_path) as writer:
        out.to_excel(writer, index=False, sheet_name="HASIL_BERSIH")

        audit = work.copy()
        audit.insert(0, "No", range(1, len(audit)+1))
        audit_out = audit[[
            "No", "RowID",
            "Nama", "Nama_Final",
            "Nomor Hp", "HP_Final",
            "Alamat", "Alamat_Final",
            "Keterangan", "Keterangan_Final",
            "Jumlah Ekor", "cluster_id"
        ]]
        audit_out.to_excel(writer, index=False, sheet_name="AUDIT_INPUT")

    print("\n✅ Selesai!")
    print("📄 File Excel:", xlsx_path)

if __name__ == "__main__":
    main()
