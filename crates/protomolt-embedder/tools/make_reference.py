import json, numpy as np
from model2vec import StaticModel

MODEL = "model"
m = StaticModel.from_pretrained(MODEL)
emb = np.asarray(m.embedding, dtype=np.float32)
print("table:", emb.shape, emb.dtype, "config normalize:", m.normalize)

texts = [
    "The quick brown fox jumps over the lazy dog.",
    "Café Zürich naïve résumé",
    "unhappiness antidisestablishmentarianism",
    "MORTGAGE Home LOAN application",
    "punctuation,stays-attached (including) §1983",
    "價格 日本語テスト mixed CJK",
    "word­join zero​width spaces here",
    "İstanbul Maße STRASSE",
    "3.14159 v2.0 released 2026-09-01",
    "floccinaucinihilipilification qqqxyzzzq",
    "😀 emoji ba😀ing test",
    "home loan refinancing rates dropped again this quarter, according to the latest survey of national lenders.",
]

vecs = m.encode(texts)
tok = m.tokenizer

# Which token stream reproduces encode()? with vs without special tokens.
out = []
verdicts = set()
for t, v in zip(texts, vecs):
    ws = tok.encode(t).ids
    ns = tok.encode(t, add_special_tokens=False).ids
    cands = {
        "no_special": ns,
        "with_special": ws,
        "no_special_no_unk": [i for i in ns if i != 1],
    }
    best = None
    for name, ids in cands.items():
        if not ids: continue
        mv = emb[ids].mean(axis=0)
        n = np.linalg.norm(mv)
        if n > 0: mv = mv / n
        d = float(np.abs(mv - v).max())
        if best is None or d < best[1]: best = (name, d, ids)
    verdicts.add(best[0])
    out.append({
        "text": t,
        "pooling": best[0],
        "max_abs_diff_reconstruct": best[1],
        "ids": best[2],
        "tokens": tok.encode(t, add_special_tokens=False).tokens,
        "vector": [float(x) for x in v],
    })
    print(f"{best[0]:>18} diff={best[1]:.2e} ntok={len(best[2]):3d} {t[:48]!r}")

print("POOLING VERDICT:", verdicts)
json.dump(out, open("reference.json", "w"))
print("wrote reference.json")
