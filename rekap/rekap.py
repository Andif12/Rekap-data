import os, re, sys
import pandas as pd

# ====== PATH INPUT/OUTPUT ======
INPUT_PATH = r"E:\KULIAH FIRA\Kerja Praktik\New folder\Data 2023.xlsx"
OUT_DIRNAME = "REKAP_HASIL"
OUT_XLSX   = "rekap_FINAL_2023_fiks.xlsx"

# ====== UTIL ======
def read_any_excel_smart(path: str) -> pd.DataFrame:
    """
    Baca Excel:
    1) Coba header=0 (default)
    2) Jika ada kolom non-string / tidak cocok, baca ulang header=None dengan nama kolom eksplisit
    """
    ext = os.path.splitext(path)[1].lower()
    engine = "xlrd" if ext == ".xls" else "openpyxl"

    try:
        df = pd.read_excel(path, engine=engine)  # header=0 (default)
    except Exception as e:
        print(f"[WARN] Gagal baca header=0 ({e}), fallback header=None + fixed names")
        df = pd.read_excel(
            path, engine=engine, header=None,
            names=["No", "Nama", "Nomor Hp", "Alamat", "Jumlah Ekor"],
            usecols=[0,1,2,3,4]
        )
        return df

    # Cek apakah seluruh nama kolom adalah string
    all_str = all(isinstance(c, str) for c in df.columns)

    # Cek apakah nama kolom 'mirip' dengan ekspektasi
    expected = {"no", "nama", "nomor hp", "alamat", "jumlah ekor"}
    def norm_colname(c):
        s = str(c)
        s = re.sub(r"\s+", " ", s.strip())
        return s.lower()
    norm_set = {norm_colname(c) for c in df.columns}

    looks_ok = len(expected.intersection(norm_set)) >= 3  # minimal 3 cocok

    if not all_str or not looks_ok:
        # Fallback: baca ulang tanpa header pakai nama kolom tetap
        df2 = pd.read_excel(
            path, engine=engine, header=None,
            names=["No", "Nama", "Nomor Hp", "Alamat", "Jumlah Ekor"],
            usecols=[0,1,2,3,4]
        )
        return df2

    return df

def normalize_name_keep(raw: str) -> str:
    if pd.isna(raw):
        return ""
    return str(raw).strip().upper()  # TIDAK split '/', TIDAK buang angka

# Nomor Hp: jadikan digit, paksa 08...
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

def most_frequent_str(series: pd.Series) -> str:
    s = series.dropna().astype(str).str.strip()
    if s.empty:
        return ""
    counts = s.value_counts()
    top = counts.max()
    candidates = [v for v, c in counts.items() if c == top and v != ""]
    candidates.sort(key=lambda x: (len(x), x))
    return candidates[0] if candidates else ""

def main():
    if not os.path.exists(INPUT_PATH):
        print(f"[ERROR] File tidak ditemukan: {INPUT_PATH}")
        sys.exit(1)

    print("[INFO] Membaca Excel ...")
    df = read_any_excel_smart(INPUT_PATH)

    # Normalisasi nama kolom -> dict lower->original (aman untuk non-string)
    def norm_colname(c):
        s = str(c)
        s = re.sub(r"\s+", " ", s.strip())
        return s.lower()

    lower_cols = {norm_colname(c): c for c in df.columns}

    def pick(*cands):
        for nm in cands:
            if nm in lower_cols:
                return lower_cols[nm]
        return None

    col_no     = pick("no","nomor","index")
    col_nama   = pick("nama","name") or "Nama"
    col_hp     = pick("nomor hp","no hp","hp","telepon","phone","no. hp") or "Nomor Hp"
    col_alamat = pick("alamat","address","lokasi") or "Alamat"
    col_jml    = pick("jumlah ekor","jumlah","qty","kuantitas") or "Jumlah Ekor"

    # Bangun dataframe kerja
    work = pd.DataFrame({
        "No": df[col_no] if col_no in df.columns else pd.Series(range(1, len(df)+1)),
        "Nama": df[col_nama] if col_nama in df.columns else "",
        "Nomor Hp": df[col_hp] if col_hp in df.columns else "",
        "Alamat": df[col_alamat] if col_alamat in df.columns else "",
        "Jumlah Ekor": df[col_jml] if col_jml in df.columns else 0,
    })

    # Normalisasi sesuai aturan
    work["Nama_Final"]   = work["Nama"].apply(normalize_name_keep)
    work["HP_Final"]     = work["Nomor Hp"].apply(normalize_phone_0x)
    work["Alamat_Final"] = work["Alamat"].apply(normalize_address)
    work["Jumlah Ekor"]  = pd.to_numeric(work["Jumlah Ekor"], errors="coerce").fillna(0)

    # Kunci penggabungan
    def build_group_key(row):
        if row["HP_Final"]:
            return f"{row['Nama_Final']}|P|{row['HP_Final']}"
        elif row["Alamat_Final"]:
            return f"{row['Nama_Final']}|A|{row['Alamat_Final']}"
        else:
            return f"{row['Nama_Final']}|R|{row.name}"  # unik per baris

    work["group_key"] = work.apply(build_group_key, axis=1)

    grouped = (
        work.groupby("group_key", as_index=False)
        .agg(
            Nama=("Nama_Final", most_frequent_str),
            NomorHp_Agregat=("HP_Final", most_frequent_str),
            Alamat_Agregat=("Alamat_Final", most_frequent_str),
            Jumlah_Ekor=("Jumlah Ekor", "sum")
        )
    )

    # Output akhir
    out = grouped.rename(columns={"Jumlah_Ekor": "Jumlah Ekor"}).copy()
    out["Nomor Hp"] = out["NomorHp_Agregat"]
    out["Alamat"]   = out["Alamat_Agregat"]
    out = out.loc[:, ["Nama", "Nomor Hp", "Alamat", "Jumlah Ekor"]]
    out.insert(0, "No", range(1, len(out)+1))

    out.sort_values(by=["Nama", "Nomor Hp", "Alamat"], inplace=True, kind="stable")
    out.reset_index(drop=True, inplace=True)
    out["No"] = range(1, len(out)+1)

    # Simpan
    out_dir = os.path.join(os.path.dirname(INPUT_PATH), OUT_DIRNAME)
    os.makedirs(out_dir, exist_ok=True)
    xlsx_path = os.path.join(out_dir, OUT_XLSX)

    with pd.ExcelWriter(xlsx_path) as writer:
        out.to_excel(writer, index=False, sheet_name="HASIL_BERSIH")

        # Sheet audit (hindari duplikasi kolom 'No')
        audit = work.copy()
        if "No" not in audit.columns:
            audit.insert(0, "No", range(1, len(audit)+1))
        audit_out = audit[[
            "No", "Nama", "Nama_Final",
            "Nomor Hp", "HP_Final",
            "Alamat", "Alamat_Final",
            "Jumlah Ekor", "group_key"
        ]].copy()
        audit_out.to_excel(writer, index=False, sheet_name="AUDIT_INPUT")

    print("\n✅ Selesai!")
    print("📄 XLSX:", xlsx_path)

if __name__ == "__main__":
    main()
