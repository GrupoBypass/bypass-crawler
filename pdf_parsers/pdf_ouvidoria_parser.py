"""
Script para extrair a tabela "TIPOLOGIA DAS MANIFESTAÇÕES — TOTAIS" dos relatórios da Ouvidoria.
Funciona para PDFs com texto e PDFs imagem (via OCR).
Lógica: tenta leitura por layout → texto → OCR (Tesseract).
Ignora linhas com '%' para evitar captura dos valores do gráfico.
Sempre retorna as 7 categorias + TOTAL GERAL (calculado se a linha não existir).

Dependências:
  pip install pdfplumber pillow pytesseract
Tesseract (necessário para PDFs imagem):
  Windows: instalar "Tesseract-OCR"
  Ubuntu: sudo apt install tesseract-ocr tesseract-ocr-por
  Mac: brew install tesseract

Exemplo de uso:
  python pdf_ouvidoria_parser.py data/ouvidoria_2025_09.pdf --page-index 3 -o data/tipologia.csv
  (PDF imagem) adicionar:
  --force-ocr --tesseract "C:\Program Files\Tesseract-OCR\tesseract.exe"

Comando completo:
python .\pdf_parsers\pdf_ouvidoria_parser.py .\data\ouvidoria_2025_09.pdf `
>>   -o .\data\tipologia_totais_2025_09.csv `
>>   --force-ocr --lang por `
>>   --tesseract "C:\Program Files\Tesseract-OCR\tesseract.exe" `
>>   --page-index 3 `
>>   --debug-ocr-text .\data\ocr_p4.txt
"""

import argparse, re, unicodedata
import pandas as pd

HEADER_RX = re.compile(r"TIPOLOGIA DAS MANIFESTA(?:C|Ç)ÕES", re.IGNORECASE)
TOTAL_GERAL_RX = re.compile(r"TOTAL\s+GERAL", re.IGNORECASE)

ORDER = [
    "Pedido de acesso à informação",
    "Reclamação",
    "Solicitação de providência",
    "Elogio",
    "Sugestão",
    "Denúncia",
    "Agradecimento",
    "TOTAL GERAL",
]


def _strip_accents(s: str) -> str:
    import unicodedata
    return "".join(c for c in unicodedata.normalize("NFKD", s) if not unicodedata.combining(c))

def _ascii(s: str) -> str:
    return _strip_accents(s or "").lower()

def import_pdfplumber():
    try:
        import pdfplumber
        return pdfplumber
    except Exception as e:
        raise SystemExit("Instale pdfplumber: pip install pdfplumber") from e

def try_import_pytesseract():
    try:
        import pytesseract
        return pytesseract
    except Exception:
        return None

def _expected_labels():
    return {
        "Pedido de acesso à informação": [
            "Pedido de acesso à informação","Pedido de acesso a informacao",
            "Pedido deacesso a informacao","Pedidodeacessoa informacao",
        ],
        "Reclamação": ["Reclamação","Reclamacao","Reclamac ao","Reclamacoa","Reclamagao"],
        "Solicitação de providência": [
            "Solicitação de providência","Solicitação de providencias",
            "Solicitacao de providencia","Solicitacao de providencias",
            "Solicitagao de providencia","Solicitagao de providência",
        ],
        "Elogio": ["Elogio","E|ogio","Elog1o","Flogio","Fogio"],
        "Sugestão": ["Sugestão","Sugestao","Sugestio"],
        "Denúncia": ["Denúncia","Denuncia","Denuncia*","Denunc1a","Denuncia.","Denuncia_"],
        "Agradecimento": ["Agradecimento","Aqradecimento"],
    }

def _build_fuzzy_label_regex(label_ascii: str) -> re.Pattern:
    import re
    tokens = [t for t in re.split(r"\s+", label_ascii.strip()) if t]
    pattern = r"\b" + r"\W{0,40}".join(map(re.escape, tokens)) + r"\b"
    return re.compile(pattern, re.IGNORECASE)


def find_candidate_pages(pdf, forced_index=None):
    if forced_index is not None and 0 <= forced_index < len(pdf.pages):
        return [forced_index]
    cands = []
    for i, page in enumerate(pdf.pages):
        t = page.extract_text() or ""
        if HEADER_RX.search(t) and TOTAL_GERAL_RX.search(t):
            cands.append(i)
    if not cands:
        for i, page in enumerate(pdf.pages):
            t = page.extract_text() or ""
            if HEADER_RX.search(t):
                cands.append(i)
    if not cands and len(pdf.pages) >= 4:
        cands = [3]
    return cands or [0]


def parse_by_layout(page) -> pd.DataFrame:
    """
    Usa pdfplumber.extract_words para reconstruir as linhas reais,
    ignora linhas com '%' e extrai o último número da linha do rótulo.
    """
    words = page.extract_words(x_tolerance=2, y_tolerance=3, keep_blank_chars=False, use_text_flow=True) or []
    if not words:
        return pd.DataFrame()

    # agrupa por linha pela coordenada 'top' arredondada
    from collections import defaultdict
    lines = defaultdict(list)
    for w in words:
        lines[round(w["top"], 1)].append(w)
    ys = sorted(lines.keys())

    def line_text(ws):
        return " ".join(w["text"] for w in sorted(ws, key=lambda x: x["x0"]))

    labels_map = _expected_labels()
    found = {}

    # percorre linhas ignorando qualquer uma que contenha %
    for y in ys:
        ws = sorted(lines[y], key=lambda x: x["x0"])
        s_raw = line_text(ws)
        if "%" in s_raw:
            continue
        s = _strip_accents(s_raw)

        # tenta casar cada rótulo nesta linha
        for canon, variants in labels_map.items():
            if canon in found:
                continue
            for v in variants:
                rx = _build_fuzzy_label_regex(_strip_accents(v))
                if rx.search(s):
                    # pega o último número da linha
                    mnums = list(re.finditer(r"\b(\d{1,6})\b", s_raw))
                    if mnums:
                        found[canon] = int(mnums[-1].group(1))
                        break

    # TOTAL GERAL: procure a linha e extraia; se não achar, some
    total = None
    for y in ys:
        ws = sorted(lines[y], key=lambda x: x["x0"])
        s_raw = line_text(ws)
        if re.search(TOTAL_GERAL_RX, s_raw):
            m = re.search(r"\b(\d{1,6})\b", s_raw)
            if m:
                total = int(m.group(1))
            break

    rows = [{"categoria": k, "quantidade": v} for k, v in found.items()]
    if not rows:
        return pd.DataFrame()

    df = pd.DataFrame(rows)
    if total is None:
        total = int(df["quantidade"].sum())
    df.loc[len(df)] = {"categoria": "TOTAL GERAL", "quantidade": int(total)}
    return df


def parse_from_text(text: str) -> pd.DataFrame:
    if not text:
        return pd.DataFrame()
    # remove linhas com % (legendas do gráfico)
    lines = [ln for ln in text.splitlines() if "%" not in ln]
    section = "\n".join(lines)

    row_rx = re.compile(
        r"^(?P<label>"
        r"Pedido de acesso à informa(?:ç|c)ão|Pedido de acesso a informacao|"
        r"Reclama(?:ç|c)ão|Reclamacao|"
        r"Solicita(?:ç|c)ão de provid(?:ê|e)n(?:cia|cias)|Solicitacao de providencia(?:s)?|"
        r"Elogio|Sugest(?:ã|a)o|Den(?:ú|u)ncia(?:\*?[¹²³]?)?|Agradecimento"
        r")\s+(?P<num>\d+)\s*$",
        re.IGNORECASE
    )

    rows, total = [], None
    for raw in section.splitlines():
        s = raw.strip()
        if not s:
            continue
        if TOTAL_GERAL_RX.search(s):
            m = re.search(r"\b(\d{1,6})\b", s)
            if m:
                total = int(m.group(1))
            continue
        m = row_rx.match(s)
        if m:
            label = re.sub(r"\*+[\d¹²³]*", "", m.group("label")).strip()
            rows.append({"categoria": label, "quantidade": int(m.group("num"))})

    if not rows:
        return pd.DataFrame()

    df = pd.DataFrame(rows)
    if total is None:
        total = int(df["quantidade"].sum())
    df.loc[len(df)] = {"categoria": "TOTAL GERAL", "quantidade": int(total)}
    return df


def _group_lines_from_tsv(data):
    groups = {}
    order = []
    n = len(data["text"])
    for i in range(n):
        if int(data.get("conf", ["0"] * n)[i]) < 0:
            continue
        key = (data.get("page_num", [0]*n)[i],
               data.get("block_num", [0]*n)[i],
               data.get("par_num", [0]*n)[i],
               data.get("line_num", [0]*n)[i])
        if key not in groups:
            groups[key] = []
            order.append(key)
        groups[key].append(i)
    return [(k, groups[k]) for k in order]

def _line_text_ascii(words_idxs, data):
    return _ascii(" ".join(data["text"][i] for i in words_idxs if data["text"][i]))

def _numeric_tokens(data, min_digits=1, img_w=1000):
    out = []
    n = len(data.get("text", []))
    for i in range(n):
        w = data["text"][i]
        if not w or not re.fullmatch(r"\d{1,6}", w):
            continue
        left  = int(data["left"][i])
        top   = int(data["top"][i])
        h     = int(data["height"][i])
        bot   = top + h
        out.append((int(w), left, top, bot))
    return out

def _overlap(a_top, a_bot, b_top, b_bot):
    return not (a_bot <= b_top or b_bot <= a_top)

def parse_by_ocr(page, lang_list, tesseract_cmd=None, debug_txt_path=None) -> pd.DataFrame:
    pytesseract = try_import_pytesseract()
    if pytesseract is None:
        return pd.DataFrame()
    if tesseract_cmd:
        pytesseract.pytesseract.tesseract_cmd = tesseract_cmd

    # rasterização em grayscale
    try:
        img = page.to_image(resolution=420).original
    except Exception:
        return pd.DataFrame()

    from PIL import ImageOps, ImageEnhance
    g = ImageOps.grayscale(img)
    g = ImageEnhance.Contrast(g).enhance(1.6)
    g = ImageEnhance.Sharpness(g).enhance(1.3)
    b = g

    base_psms = [6, 4, 11]
    cfg = lambda psm: fr'--oem 3 --psm {psm} -c preserve_interword_spaces=1'
    labels_map = _expected_labels()

    def read_tsv(lang, psm):
        return pytesseract.image_to_data(b, lang=lang, config=cfg(psm), output_type=pytesseract.Output.DICT)

    last_data = None
    for lang in lang_list:
        data = None
        for psm in base_psms:
            try:
                data = read_tsv(lang, psm)
                if any(data.get("text", [])):
                    last_data = data
                    break
            except Exception:
                data = None
        if data is None or not any(data.get("text", [])):
            continue

        # Debug opcional
        if debug_txt_path:
            try:
                import csv, os
                tsv_out = os.path.splitext(debug_txt_path)[0] + ".tsv"
                headers = ["level","page_num","block_num","par_num","line_num","word_num",
                           "left","top","width","height","conf","text"]
                with open(tsv_out, "w", newline="", encoding="utf-8") as f:
                    w = csv.writer(f, delimiter="\t"); w.writerow(headers)
                    n = len(data["text"])
                    for i in range(n):
                        row = [data.get(k, [None]*n)[i] for k in headers]
                        w.writerow(row)
                with open(debug_txt_path, "w", encoding="utf-8") as ftxt:
                    ftxt.write("\n".join(t for t in data["text"] if t))
                print(f"[DEBUG] TSV salvo em: {tsv_out}")
                print(f"[DEBUG] Texto OCR salvo em: {debug_txt_path}")
            except Exception as e:
                print(f"[DEBUG] Falha ao salvar TSV/TXT: {e}")

        # Constrói linhas e ignora linhas com %
        line_groups = _group_lines_from_tsv(data)
        lines_meta = []
        n = len(data.get("text", []))
        conf_list = data.get("conf", ["0"] * n)
        for key, idxs in line_groups:
            idxs = [i for i in idxs if data["text"][i] and int(conf_list[i]) >= 0]
            if not idxs:
                continue
            txt_line = " ".join(data["text"][i] for i in sorted(idxs))
            if "%" in txt_line:  # ignora legendas com percentual
                continue
            lefts  = [int(data["left"][i]) for i in idxs]
            rights = [int(data["left"][i]) + int(data["width"][i]) for i in idxs]
            tops   = [int(data["top"][i]) for i in idxs]
            bots   = [int(data["top"][i]) + int(data["height"][i]) for i in idxs]
            l, t, r, btm = min(lefts), min(tops), max(rights), max(bots)
            h = btm - t
            lines_meta.append((key, t, btm, l, r, h, idxs, txt_line))

        all_nums = _numeric_tokens(data)

        def rightmost_num_in_band(y_top, y_bot, x_min=None):
            c = [(v, lx) for (v, lx, nt, nb) in all_nums
                 if _overlap(nt, nb, y_top, y_bot) and (x_min is None or lx >= x_min)]
            return max(c, key=lambda t: t[1])[0] if c else None

        found = {}
        total = None

        # varre rótulos
        for canon, variants in labels_map.items():
            for (key, top, bot, lft, rgt, h, idxs, txt_line) in lines_meta:
                ltxt = _ascii(txt_line)
                if any(_build_fuzzy_label_regex(_ascii(v)).search(ltxt) for v in variants):
                    y_top = top - int(0.25 * h)
                    y_bot = bot + int(0.25 * h)
                    # pega o número mais à direita da banda da linha (evita pegar '4' aleatórios)
                    q = rightmost_num_in_band(y_top, y_bot, x_min=rgt + 10)
                    if q is None:
                        q = rightmost_num_in_band(y_top, y_bot)
                    if q is not None:
                        found[canon] = q
                        break

        # TOTAL GERAL
        for (key, top, bot, lft, rgt, h, idxs, txt_line) in lines_meta:
            if re.search(TOTAL_GERAL_RX, txt_line):
                y_top = top - int(0.25 * h)
                y_bot = bot + int(0.25 * h)
                total = rightmost_num_in_band(y_top, y_bot, x_min=rgt + 5)
                if total is None:
                    total = rightmost_num_in_band(y_top, y_bot)
                break

        if found:
            df = pd.DataFrame([{"categoria": k, "quantidade": v} for k, v in found.items()])
            if total is None:
                total = int(df["quantidade"].sum())
            df.loc[len(df)] = {"categoria": "TOTAL GERAL", "quantidade": int(total)}
            return df

    # Fallback: reaproveita o texto do OCR e aplica parser de layout-like
    try:
        if last_data:
            flat = " ".join(t for t in last_data.get("text", []) if t)
            # remove linhas com %
            lines = [ln for ln in flat.splitlines() if "%" not in ln]
            df = parse_from_text("\n".join(lines))
            if not df.empty:
                return df
    except Exception:
        pass

    return pd.DataFrame()


def normalize_and_sort(df: pd.DataFrame) -> pd.DataFrame:
    if df.empty:
        return df
    df["categoria"] = (
        df["categoria"].astype(str)
        .str.replace(r"\s+", " ", regex=True)
        .str.replace("Solicitação de providências", "Solicitação de providência", regex=False)
        .str.replace("Denúncia*1", "Denúncia", regex=False)
        .str.replace("Denuncia*1", "Denúncia", regex=False)
        .str.replace("Denúncia*¹", "Denúncia", regex=False)
        .str.replace("Denuncia*¹", "Denúncia", regex=False)
        .str.strip()
    )
    try:
        cat_type = pd.CategoricalDtype(ORDER, ordered=True)
        df["categoria"] = df["categoria"].astype(cat_type)
        df = df.sort_values("categoria").reset_index(drop=True)
    except Exception:
        pass
    return df


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("pdf", help="Caminho do PDF")
    ap.add_argument("-o","--output", default="tipologia_totais_p4.csv", help="CSV de saída")
    ap.add_argument("--force-ocr", action="store_true", help="Força OCR (Tesseract)")
    ap.add_argument("--lang", default="por", help="Idioma principal do OCR (ex.: por, eng)")
    ap.add_argument("--tesseract", default=None, help="Caminho completo do tesseract.exe")
    ap.add_argument("--page-index", type=int, default=None, help="Força um índice de página (0-based)")
    ap.add_argument("--debug-ocr-text", default=None, help="Salva o texto OCR em um .txt (debug)")
    args = ap.parse_args()

    pdfplumber = import_pdfplumber()
    with pdfplumber.open(args.pdf) as pdf:
        pages = find_candidate_pages(pdf, forced_index=args.page_index)
        df_final = pd.DataFrame()

        for idx in pages:
            page = pdf.pages[idx]

            # 1) PRIMEIRA TENTATIVA: LAYOUT (mais robusto neste PDF)
            df = parse_by_layout(page)
            if not df.empty:
                df_final = df
                break

            # 2) Se não deu por layout e não forçar OCR, tente texto simples filtrado
            if not args.force_ocr:
                t = page.extract_text() or ""
                df = parse_from_text(t)
                if not df.empty:
                    df_final = df
                    break

            # 3) OCR
            langs = [args.lang] if args.lang else []
            if "eng" not in langs:
                langs.append("eng")
            df = parse_by_ocr(page, langs, tesseract_cmd=args.tesseract, debug_txt_path=args.debug_ocr_text)
            if not df.empty:
                df_final = df
                break

    if df_final.empty:
        print("[DEBUG] Nada extraído. Possíveis causas: linhas com % ofuscaram os números, rótulos divergentes ou layout inesperado.")
        print("[DEBUG] Tente --force-ocr com --debug-ocr-text para inspecionar o TSV/TXT.")
        raise SystemExit("Falha ao extrair a tabela. Veja as dicas de debug.")

    df_final = normalize_and_sort(df_final)
    # Validação: soma das categorias = TOTAL
    try:
        tot_row = df_final.loc[df_final["categoria"]=="TOTAL GERAL", "quantidade"]
        if not tot_row.empty:
            total = int(tot_row.iloc[0])
            soma = int(df_final.loc[df_final["categoria"]!="TOTAL GERAL","quantidade"].sum())
            if soma != total:
                print(f"[WARN] Soma das categorias ({soma}) != TOTAL GERAL ({total}). Verifique OCR/labels.")
    except Exception:
        pass

    df_final.to_csv(args.output, index=False, encoding="utf-8-sig")
    print(f"OK: {args.output}")

if __name__ == "__main__":
    main()
