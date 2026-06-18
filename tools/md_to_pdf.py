"""
md_to_pdf.py — Convertit un fichier Markdown en PDF professionnel via xhtml2pdf.
Usage : python md_to_pdf.py <input.md> [output.pdf]
"""

import sys
import os
import markdown
from xhtml2pdf import pisa

CSS_STYLE = """
@page {
    size: A4;
    margin: 18mm 16mm 22mm 16mm;
    @frame footer {
        -pdf-frame-content: footerContent;
        bottom: 8mm;
        margin-left: 16mm;
        margin-right: 16mm;
        height: 10mm;
    }
}

body {
    font-family: Helvetica, Arial, sans-serif;
    font-size: 10pt;
    line-height: 1.55;
    color: #1a1a2e;
}

/* TITRES */
h1 {
    font-size: 20pt;
    font-weight: bold;
    color: #1a3c6e;
    border-bottom: 3pt solid #1a3c6e;
    padding-bottom: 5pt;
    margin-top: 0;
    margin-bottom: 4pt;
}
h2 {
    font-size: 13pt;
    font-weight: bold;
    color: #2d6cb4;
    border-left: 4pt solid #2d6cb4;
    padding-left: 8pt;
    margin-top: 18pt;
    margin-bottom: 8pt;
    -pdf-keep-with-next: true;
}
h3 {
    font-size: 11pt;
    font-weight: bold;
    color: #1a3c6e;
    margin-top: 14pt;
    margin-bottom: 6pt;
    -pdf-keep-with-next: true;
}
h4 {
    font-size: 10pt;
    font-weight: bold;
    color: #444;
    margin-top: 10pt;
    margin-bottom: 4pt;
    -pdf-keep-with-next: true;
}

/* PARAGRAPHES & LISTES */
p { margin: 0 0 7pt 0; }
ul, ol { margin: 4pt 0 8pt 0; padding-left: 18pt; }
li { margin-bottom: 3pt; }
strong { color: #1a3c6e; }
em { color: #555; }

/* TABLEAUX */
table {
    width: 100%;
    border-collapse: collapse;
    margin: 10pt 0 14pt 0;
    font-size: 8.5pt;
}
thead tr { background-color: #1a3c6e; }
thead th {
    color: #ffffff;
    padding: 5pt 7pt;
    text-align: left;
    font-weight: bold;
    border: 1pt solid #1a3c6e;
}
tbody tr:nth-child(even) { background-color: #f0f4fa; }
tbody tr:nth-child(odd)  { background-color: #ffffff; }
tbody td {
    padding: 5pt 7pt;
    border: 1pt solid #c8d4e8;
    vertical-align: top;
}

/* BLOCS DE CODE */
pre {
    background-color: #1e2740;
    color: #c8d4f4;
    padding: 10pt 12pt;
    font-size: 7.5pt;
    font-family: Courier, monospace;
    white-space: pre-wrap;
    word-wrap: break-word;
    margin: 8pt 0 12pt 0;
    border-left: 3pt solid #2d6cb4;
    border-radius: 3pt;
}
code {
    background-color: #eef2fb;
    color: #c0392b;
    font-family: Courier, monospace;
    font-size: 8.5pt;
    padding: 1pt 4pt;
}
pre code {
    background-color: transparent;
    color: inherit;
    padding: 0;
}

/* SÉPARATEUR */
hr {
    border: none;
    border-top: 1.5pt solid #c8d4e8;
    margin: 16pt 0;
}

/* BLOCKQUOTE */
blockquote {
    border-left: 3pt solid #2d6cb4;
    background-color: #eef6ff;
    margin: 8pt 0;
    padding: 6pt 12pt;
    font-style: italic;
    color: #2c3e50;
}

/* FOOTER */
#footerContent {
    font-size: 7.5pt;
    color: #888888;
    text-align: center;
    border-top: 0.5pt solid #cccccc;
    padding-top: 3pt;
}
"""


EXTENSIONS = [
    'markdown.extensions.tables',
    'markdown.extensions.fenced_code',
    'markdown.extensions.nl2br',
    'markdown.extensions.sane_lists',
]


def md_to_pdf(md_path: str, pdf_path: str):
    with open(md_path, encoding="utf-8") as f:
        md_text = f.read()

    html_body = markdown.markdown(md_text, extensions=EXTENSIONS)

    titre = os.path.splitext(os.path.basename(md_path))[0]

    html_full = f"""<!DOCTYPE html>
<html lang="fr">
<head>
  <meta charset="UTF-8">
  <title>{titre}</title>
  <style>{CSS_STYLE}</style>
</head>
<body>
<div id="footerContent">
    FinData Solutions — Module DEPE855 | EPSI Mastère Expert en Ingénierie des données 2025-2026
    &nbsp;&nbsp;|&nbsp;&nbsp;
    <pdf:pagenumber /> / <pdf:pagecount />
</div>
{html_body}
</body>
</html>"""

    with open(pdf_path, "wb") as f:
        result = pisa.CreatePDF(html_full.encode("utf-8"), dest=f, encoding="utf-8")

    if result.err:
        print(f"Erreurs xhtml2pdf : {result.err}")
        sys.exit(1)
    else:
        size_kb = os.path.getsize(pdf_path) // 1024
        print(f"PDF généré : {pdf_path}  ({size_kb} Ko)")


if __name__ == "__main__":
    if len(sys.argv) < 2:
        print("Usage: python md_to_pdf.py <input.md> [output.pdf]")
        sys.exit(1)
    inp = sys.argv[1]
    out = sys.argv[2] if len(sys.argv) > 2 else os.path.splitext(inp)[0] + ".pdf"
    md_to_pdf(inp, out)
