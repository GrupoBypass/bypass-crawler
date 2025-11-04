#TODO: Entender pq ele não tá conseguindo puxar os PDFs...

import re
import os
import sys
import json
import time
import math
import shutil
import logging
from pathlib import Path
from datetime import datetime
from dateutil.relativedelta import relativedelta

import requests
from bs4 import BeautifulSoup
import pdfplumber
import pandas as pd

SESSION = requests.Session()
SESSION.headers.update({
    "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
                  "AppleWebKit/537.36 (KHTML, like Gecko) "
                  "Chrome/120.0 Safari/537.36"
})

DATASET_URL = "https://transparencia.metrosp.com.br/dataset/ouvidoria"
OUT_DIR = Path(__file__).resolve().parents[1] / "data"
OUT_DIR.mkdir(parents=True, exist_ok=True)

MES_PT = {
    1: "janeiro", 2: "fevereiro", 3: "março", 4: "abril", 5: "maio", 6: "junho",
    7: "julho", 8: "agosto", 9: "setembro", 10: "outubro", 11: "novembro", 12: "dezembro"
}
MES_PT_NORMALIZADO = {v.replace("ç","c"): k for k,v in MES_PT.items()}

CKAN_PACKAGE_SHOW = "https://transparencia.metrosp.com.br/api/3/action/package_show?id=ouvidoria"

def month_tokens_pt(mes_pt):
    # gera variações aceitáveis do mês (Setembro, setembro)
    base = (mes_pt or "").strip()
    return {base, base.capitalize(), base.lower(), base.upper()}

def ckan_list_resources():
    r = SESSION.get(CKAN_PACKAGE_SHOW, timeout=30)
    r.raise_for_status()
    data = r.json()
    if not data.get("success"):
        return []
    return data["result"].get("resources", []) or []

def ckan_find_month_resources(alvo_mes, alvo_ano):
    """Filtra os resources da API por mês/ano (tolerante a hífen, espaços, acentos)."""
    tokens = month_tokens_pt(alvo_mes)
    year_str = str(alvo_ano)
    hits = []

    for res in ckan_list_resources():
        # alguns campos úteis que podem existir
        title = (res.get("title") or res.get("name") or "").strip()
        desc  = (res.get("description") or "").strip()
        url   = (res.get("url") or "").strip()
        fmt   = (res.get("format") or "").strip().lower()

        text = f"{title} {desc}".lower()

        # precisa indicar ser o relatório mensal de ouvidoria do mês/ano
        cond_mes = any(t.lower() in text for t in tokens)
        cond_ano = year_str in text
        cond_ouvidoria = ("ouvidoria" in text)
        cond_relatorio = ("relatório mensal" in text) or ("relatorio mensal" in text)

        if cond_mes and cond_ano and cond_ouvidoria and cond_relatorio:
            # priorize PDFs ou "node/*/download"
            if fmt == "pdf" or url.lower().endswith(".pdf") or "/node/" in url:
                hits.append({
                    "title": title, "url": url, "format": fmt,
                    "page": res.get("page_url") or "",  # alguns CKANs expõem
                    "id": res.get("id"),
                })

    # ordena para priorizar PDFs explícitos
    hits.sort(key=lambda x: (x["format"] != "pdf", "/node/" not in x["url"]))
    return hits


def month_minus_two(today=None):
    today = today or datetime.now()
    target = today - relativedelta(months=2)
    return target.year, target.month, MES_PT[target.month]

def fetch(url, **kwargs):
    r = SESSION.get(url, timeout=30, allow_redirects=True, **kwargs)
    r.raise_for_status()
    return r


def find_month_resources_html(html, alvo_mes, alvo_ano):
    """Fallback via HTML se a API CKAN não retornar nada."""
    soup = BeautifulSoup(html, "lxml")
    anchors = soup.find_all("a", href=True)

    mes_token = (alvo_mes or "").strip()
    year_str = str(alvo_ano)

    results = []
    for a in anchors:
        txt = (a.get_text() or "").strip()
        href = a["href"]
        txt_norm = " ".join(txt.split()).lower()

        # exige "ouvidoria", mês e ano no texto, e 'relatório mensal' (com ou sem acento)
        if ("ouvidoria" in txt_norm and
            mes_token.lower() in txt_norm and
            year_str in txt_norm and
            ("relatório mensal" in txt_norm or "relatorio mensal" in txt_norm)):

            if href.startswith("/"):
                href = "https://transparencia.metrosp.com.br" + href
            if "/dataset/ouvidoria/resource/" in href:
                results.append(href)

    # remove duplicados preservando ordem
    return list(dict.fromkeys(results))




def resolve_download_link(resource_page_html, base_url="https://transparencia.metrosp.com.br"):
    """
    Na página do recurso, tenta:
      1) Link 'Baixar' (/node/{id}/download)
      2) Qualquer link direto para PDF em /sites/default/files/...
    Retorna URL absoluta.
    """
    soup = BeautifulSoup(resource_page_html, "lxml")

    for a in soup.find_all("a", href=True):
        label = (a.get_text() or "").strip().lower()
        href = a["href"]
        if "baixar" in label or "download" in label or "/node/" in href and "/download" in href:
            if href.startswith("/"):
                return base_url + href
            return href

    for a in soup.find_all("a", href=True):
        href = a["href"]
        if href.lower().endswith(".pdf"):
            if href.startswith("/"):
                return base_url + href
            return href

    return None

def is_pdf_bytes(content: bytes) -> bool:
    return content[:5] == b"%PDF-"

def absolutize(url: str) -> str:
    if url.startswith("http"):
        return url
    if url.startswith("/"):
        return "https://transparencia.metrosp.com.br" + url
    return url

def try_extract_pdf_link_from_html(html: str) -> str | None:
    soup = BeautifulSoup(html, "lxml")
    for a in soup.find_all("a", href=True):
        href = a["href"]
        if href.lower().endswith(".pdf"):
            return absolutize(href)
    import re
    m = re.search(r'href=["\']([^"\']+\.pdf)["\']', html, re.IGNORECASE)
    if m:
        return absolutize(m.group(1))
    return None

def download_pdf_or_follow(resource_url: str, download_url: str, referer: str | None, out_path: Path) -> Path:
    headers = {}
    if referer:
        headers["Referer"] = referer

    r = SESSION.get(download_url, headers=headers, timeout=30, allow_redirects=True)
    r.raise_for_status()
    blob = r.content

    if is_pdf_bytes(blob) or "application/pdf" in r.headers.get("Content-Type","").lower():
        out_path.write_bytes(blob)
        return out_path

    html = blob.decode(errors="ignore")
    maybe = try_extract_pdf_link_from_html(html)
    if maybe:
        r2 = SESSION.get(maybe, headers=headers, timeout=30, allow_redirects=True)
        r2.raise_for_status()
        blob2 = r2.content
        if is_pdf_bytes(blob2) or "application/pdf" in r2.headers.get("Content-Type","").lower():
            out_path.write_bytes(blob2)
            return out_path

    res_html = fetch(resource_url).text
    alt = try_extract_pdf_link_from_html(res_html)
    if alt:
        r3 = SESSION.get(alt, headers=headers, timeout=30, allow_redirects=True)
        r3.raise_for_status()
        blob3 = r3.content
        if is_pdf_bytes(blob3) or "application/pdf" in r3.headers.get("Content-Type","").lower():
            out_path.write_bytes(blob3)
            return out_path

    raise RuntimeError("Não consegui obter um PDF válido (assinatura %PDF- ausente).")

def parse_pdf_to_row(pdf_path):
    text = ""
    with pdfplumber.open(pdf_path) as pdf:
        for page in pdf.pages:
            text += (page.extract_text(x_tolerance=1, y_tolerance=1) or "") + "\n"

    def get_month_year(t):
        m = re.search(r"(janeiro|fevereiro|mar[cç]o|abril|maio|junho|julho|agosto|setembro|outubro|novembro|dezembro)\s*/\s*(\d{4})", t, re.IGNORECASE)
        if not m: 
            return None, None
        mes = m.group(1).lower().replace("ç","c")
        ano = int(m.group(2))
        return ano, MES_PT_NORMALIZADO.get(mes)

    ano, mes_num = get_month_year(text)

    tot_manifest = None
    tot_sic = None
    tot_ovd = None

    m_intro = re.search(r"recebeu\s+(\d{1,5})\s+manifesta[cç][oõ]es.*?sendo\s+(\d{1,5})\s+demandas\s+SIC.*?e\s+(\d{1,5})\s+demandas\s+Ouvidoria", text, re.IGNORECASE | re.DOTALL)
    if m_intro:
        tot_manifest = int(m_intro.group(1))
        tot_sic = int(m_intro.group(2))
        tot_ovd = int(m_intro.group(3))
    if tot_sic is None:
        m_sic = re.search(r"\bSIC\s+(\d{1,5})", text)
        if m_sic: tot_sic = int(m_sic.group(1))
    if tot_ovd is None:
        m_ovd = re.search(r"\bOVD\s+(\d{1,5})", text)
        if m_ovd: tot_ovd = int(m_ovd.group(1))
    if tot_manifest is None:
        m_total = re.search(r"TOTAL GERAL DE MANIFESTA[CÇ][OÕ]ES.*?(\d{2,5})\s*$", text, re.IGNORECASE | re.MULTILINE)
        if m_total: tot_manifest = int(m_total.group(1))

    def find_count(label):
        m = re.search(label + r"\s+(\d+)", text, re.IGNORECASE)
        return int(m.group(1)) if m else None

    tipologia = {
        "pedido_acesso_informacao": find_count(r"Pedido de acesso à informa[cç][aã]o"),
        "reclamacao": find_count(r"Reclama[cç][aã]o"),
        "solicitacao_providencia": find_count(r"Solicita[cç][aã]o de provid[êe]ncia"),
        "elogio": find_count(r"Elogio"),
        "sugestao": find_count(r"Sugest[aã]o"),
        "denuncia": find_count(r"Den[úu]ncia"),
        "agradecimento": find_count(r"Agradecimento"),
    }

    m_bloco = re.search(r"CANAIS DE COMUNICA[cç][aã]O\s+(.*?)TEMPO DE RESPOSTA", text, re.IGNORECASE | re.DOTALL)
    bloco = m_bloco.group(1) if m_bloco else ""
    def extract_label_numbers(label):
        vals = []
        for mm in re.finditer(re.escape(label) + r"\s+(\d{1,4})(?!\s*%)", bloco, re.IGNORECASE):
            vals.append(int(mm.group(1)))
        return max(vals) if vals else None

    canais = {
        "canal_falasp":             extract_label_numbers("Fala.SP"),
        "canal_fale_conosco":       extract_label_numbers("Fale Conosco"),
        "canal_central_0800":       extract_label_numbers("Central de Informações (0800)"),
        "canal_reclame_aqui":       extract_label_numbers("Reclame Aqui"),
        "canal_procon":             extract_label_numbers("Procon"),
        "canal_ministerio_publico": extract_label_numbers("Ministério Público"),
        "canal_redes_sociais":      extract_label_numbers("Redes Sociais"),
        "canal_email":              extract_label_numbers("E-mail"),
    }

    m_tempo = re.search(r"tempo m[ée]dio de resposta.*?Ouvidoria.*?(\d+)\s+dias.*?SIC.*?(\d+)\s+dias", text, re.IGNORECASE | re.DOTALL)
    t_ovd = int(m_tempo.group(1)) if m_tempo else None
    t_sic = int(m_tempo.group(2)) if m_tempo else None

    row = {
        "ano": ano, "mes": mes_num,
        "total_manifestacoes": tot_manifest,
        "total_sic": tot_sic, "total_ovd": tot_ovd,
        "tempo_medio_resposta_ovd_dias": t_ovd,
        "tempo_medio_resposta_sic_dias": t_sic,
        **tipologia, **canais
    }
    return row

def main():
    alvo_ano, alvo_mes_num, alvo_mes_pt = month_minus_two()
    print(f"Alvo: {alvo_mes_pt.capitalize()} / {alvo_ano}")

    # 1) Tenta via CKAN API
    hits = ckan_find_month_resources(alvo_mes_pt, alvo_ano)

    if not hits:
        # 2) Fallback: HTML
        ds_html = fetch(DATASET_URL).text
        recursos_html = find_month_resources_html(ds_html, alvo_mes_pt, alvo_ano)
        if not recursos_html:
            print("Nenhum recurso encontrado para o mês/ano alvo.")
            sys.exit(2)
        # converte páginas de recurso em pares {title:?, url:?}
        hits = [{"title": "recurso_html", "url": u, "format": "html", "page": u} for u in recursos_html]

    print("Recursos candidatos encontrados:")
    for h in hits:
        print(" -", h.get("title") or "(sem título)", "→", h["url"])
  

    rows = []
    sucesso = False  # opcional, para parar após o primeiro bom

    for resource_url in recursos:
        try:
            print("Recurso:", resource_url)
            res_html = fetch(resource_url).text
            download_url = resolve_download_link(res_html) or resource_url
            print("Download:", download_url)

            filename = f"ouvidoria_{alvo_ano}_{alvo_mes_num:02d}.pdf"
            pdf_path = OUT_DIR / filename

            pdf_path = download_pdf_or_follow(
                resource_url, download_url, referer=resource_url, out_path=pdf_path
            )
            print("Salvo:", pdf_path)

            row = parse_pdf_to_row(pdf_path)
            row["ano"] = row.get("ano") or alvo_ano
            row["mes"] = row.get("mes") or alvo_mes_num

            if not row.get("total_sic") and not row.get("total_ovd"):
                print("Aviso: parse sem campos essenciais, ignorando este recurso.")
                continue

            rows.append(row)
            sucesso = True

            break

        except requests.HTTPError as e:
            print(f"Aviso: HTTP {e.response.status_code} em {resource_url}. Seguindo o próximo…")
            continue
        except Exception as e:
            print(f"Aviso: falha ao processar {resource_url}: {e}. Seguindo o próximo…")
            continue

    if not rows:
        print("Nenhum PDF válido processado para o mês alvo.")
        sys.exit(2)

    df = pd.DataFrame(rows)
    csv_out = OUT_DIR / f"ouvidoria_{alvo_ano:04d}_{alvo_mes_num:02d}.csv"
    df.to_csv(csv_out, index=False, encoding="utf-8")
    print("CSV:", csv_out)


if __name__ == "__main__":
    main()
