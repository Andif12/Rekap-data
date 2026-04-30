import os, re, sys
import pandas as pd

# ==== PATH (ganti jika perlu) ====
PATH_A = r"E:\KULIAH FIRA\Kerja Praktik\New folder\Data 2023.xlsx"   # A: No, Nama, Nomor Hp, Alamat, Keterangan, Jumlah Ekor, Jumlah Transaksi
PATH_B = r"E:\KULIAH FIRA\Kerja Praktik\New folder\REKAP_HASIL\rekap_2025_September.xlsx"    # B: Nama, Wilayah, Total_Jumlah, Total_Transaksi
OUT_DIR = r"E:\KULIAH FIRA\Kerja Praktik\New folder\REKAP_HASIL\rekap_last"
OUT_XLSX = "2025_September.xlsx"

# ==== AMBANG FUZZY NAMA (ubah jika perlu) ====
THRESH_NAMA = 85 

# ==== TRY IMPORT RAPIDFUZZ ====
try:
    from rapidfuzz import fuzz, process
    HAVE_RAPIDFUZZ = True
except Exception:
    HAVE_RAPIDFUZZ = False

# ==== Utilities ====
def read_excel_smart(path: str) -> pd.DataFrame:
    ext = os.path.splitext(path)[1].lower()
    engine = "xlrd" if ext == ".xls" else "openpyxl"
    df = pd.read_excel(path, engine=engine)
    df.columns = [str(c).strip() for c in df.columns]
    return df

def norm_upper(x: str) -> str:
    if pd.isna(x): 
        return ""
    s = str(x).strip().upper()
    # rapikan spasi ganda & strip tanda hubung/komma “ringan” agar lebih robust
    s = re.sub(r"\s+", " ", s)
    s = s.replace(",", " ").replace(".", " ").replace("-", " ").replace("/", " / ")
    s = re.sub(r"\s+", " ", s).strip()
    return s

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

def pick_cols(df: pd.DataFrame, mapping: dict, required: list):
    lower = {c.lower(): c for c in df.columns}
    out = {}
    for key, cands in mapping.items():
        out[key] = None
        for c in cands:
            if c in lower:
                out[key] = lower[c]
                break
    missing = [k for k in required if not out.get(k)]
    if missing:
        print(f"[WARN] Kolom wajib hilang: {missing}")
    return out

def main():
    if not os.path.exists(PATH_A) or not os.path.exists(PATH_B):
        print("[ERROR] Periksa PATH_A / PATH_B.")
        sys.exit(1)
    os.makedirs(OUT_DIR, exist_ok=True)

    print("[INFO] Membaca A & B ...")
    dfA = read_excel_smart(PATH_A)
    dfB = read_excel_smart(PATH_B)

    # Map kolom
    mapA = pick_cols(
        dfA,
        mapping={
            "Nama": ["nama","name"],
            "Alamat": ["alamat","wilayah","address","lokasi"],
            "NomorHp": ["nomor hp","no hp","hp","telepon","phone","no. hp"],
            "Keterangan": ["keterangan","ket","catatan","note","desc","remarks"]
        },
        required=["Nama"]  # fokus nama dulu
    )
    mapB = pick_cols(
        dfB,
        mapping={
            "Nama": ["nama","name"],
            "Wilayah": ["wilayah","alamat","address","lokasi"],
            "Total_Jumlah": ["total_jumlah","total jumlah","jumlah"],
            "Total_Transaksi": ["total_transaksi","total transaksi","transaksi"]
        },
        required=["Nama","Wilayah","Total_Jumlah","Total_Transaksi"]
    )

    # ====== Donor A -> ambil data dasar per NAMA ======
    A = pd.DataFrame({
        "Nama": dfA[mapA["Nama"]] if mapA["Nama"] else "",
        "Alamat": dfA[mapA["Alamat"]] if mapA["Alamat"] else "",
        "Nomor Hp": dfA[mapA["NomorHp"]] if mapA["NomorHp"] else "",
        "Keterangan": dfA[mapA["Keterangan"]] if mapA["Keterangan"] else ""
    }).copy()
    A.reset_index(drop=True, inplace=True)
    A["RowID"] = A.index
    A["Nama_Key"] = A["Nama"].apply(norm_upper)
    A["HP_Final"] = A["Nomor Hp"].apply(normalize_phone_0x)
    A["Ket_Final"] = A["Keterangan"].astype(str).str.strip()

    # Pilih 1 baris pertama per NAMA (urutan baris di file A)
    A_name_first = (
        A[A["Nama_Key"] != ""]
        .sort_values("RowID", kind="stable")
        .groupby("Nama_Key", as_index=False)
        .first()
    )

    # ====== Target B ======
    B = pd.DataFrame({
        "Nama": dfB[mapB["Nama"]],
        "Wilayah": dfB[mapB["Wilayah"]],
        "Total_Jumlah": pd.to_numeric(dfB[mapB["Total_Jumlah"]], errors="coerce"),
        "Total_Transaksi": pd.to_numeric(dfB[mapB["Total_Transaksi"]], errors="coerce"),
    }).copy()
    B["Nama_Key"] = B["Nama"].apply(norm_upper)

    # ---- EXACT JOIN berdasarkan NAMA dulu ----
    merged = B.merge(
        A_name_first[["Nama_Key","HP_Final","Ket_Final","RowID"]],
        on="Nama_Key",
        how="left",
        suffixes=("", "_AEXACT")
    )
    merged["Nomor Hp"] = merged["HP_Final"].fillna("")
    merged["Keterangan"] = merged["Ket_Final"].fillna("")
    exact_filled_rows = (~merged["RowID"].isna()).sum()

    # ---- FUZZY NAMA untuk yang belum dapat donor ----
    fuzzy_logs = []
    if HAVE_RAPIDFUZZ:
        print(f"[INFO] RapidFuzz terdeteksi. Fuzzy matching NAMA dengan threshold {THRESH_NAMA} ...")
        # Siapkan daftar kandidat & map dari Nama_Key A -> (HP, Ket, RowID)
        a_names = A_name_first["Nama_Key"].tolist()
        a_map = {
            row["Nama_Key"]: (row["HP_Final"], row["Ket_Final"], row["RowID"])
            for _, row in A_name_first.iterrows()
        }

        need_fuzzy = merged["Nomor Hp"].eq("") & merged["Keterangan"].eq("")
        idxs = merged.index[need_fuzzy].tolist()

        for i in idxs:
            target_name = merged.at[i, "Nama_Key"]
            if not target_name:
                continue
            match = process.extractOne(
                target_name,
                a_names,
                scorer=fuzz.token_set_ratio,   # cukup robust untuk nama
            )
            if match is None:
                continue
            best_name, score, _ = match
            if score >= THRESH_NAMA:
                hp, ket, rid = a_map[best_name]
                if pd.isna(hp): hp = ""
                if pd.isna(ket): ket = ""
                if (merged.at[i, "Nomor Hp"] == "") and hp:
                    merged.at[i, "Nomor Hp"] = normalize_phone_0x(hp)
                if (merged.at[i, "Keterangan"] == "") and ket:
                    merged.at[i, "Keterangan"] = str(ket).strip()
                fuzzy_logs.append({
                    "Index_B": int(i),
                    "Nama_B": merged.at[i, "Nama"],
                    "Wilayah_B": merged.at[i, "Wilayah"],
                    "Nama_Key_B": target_name,
                    "Matched_A_Nama_Key": best_name,
                    "Score": int(score),
                    "HP_from_A": hp,
                    "Ket_from_A": ket,
                    "RowID_A": int(rid)
                })
    else:
        print("[WARN] rapidfuzz tidak tersedia. Lewati fuzzy matching. (pip install rapidfuzz)")

    # Susun hasil akhir (angka B dipertahankan)
    result = merged[[
        "Nama","Wilayah","Nomor Hp","Keterangan","Total_Jumlah","Total_Transaksi"
    ]].copy()
    result.insert(0, "No", range(1, len(result)+1))

    # Simpan
    out_path = os.path.join(OUT_DIR, OUT_XLSX)
    with pd.ExcelWriter(out_path) as writer:
        result.to_excel(writer, index=False, sheet_name="B_ENRICHED")
        merged.to_excel(writer, index=False, sheet_name="JOIN_DEBUG")
        if HAVE_RAPIDFUZZ:
            pd.DataFrame(fuzzy_logs).to_excel(writer, index=False, sheet_name="LOG_FUZZY")

    print("\n✅ Selesai! B diperkaya dari A (angka B tidak diubah).")
    print(f"   • Exact matched rows (donor ketemu via NAMA): {exact_filled_rows}")
    if HAVE_RAPIDFUZZ:
        print(f"   • Fuzzy matched rows (skor >= {THRESH_NAMA}): {len(fuzzy_logs)}")
    print(f"📄 File: {out_path}")

if __name__ == "__main__":
    main()
