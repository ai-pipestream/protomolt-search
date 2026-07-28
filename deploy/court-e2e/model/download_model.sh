#!/bin/sh
# Download a Model2Vec static embedding table from HuggingFace into the
# models volume. The OpenNLP analysis sidecar reads the Model2Vec layout
# natively (OPENNLP_EMBEDDINGS_DIR points at MODEL_OUT), so this is a
# plain file fetch — no distillation, no Python.
#
# Default table: minishlab/potion-retrieval-32M (256d WordPiece static
# embedder, MIT). Override MODEL_REPO/MODEL_FILES for another table —
# e.g. a bge-m3 distillation once the Java distiller produces it.
set -eu

: "${MODEL_REPO:=minishlab/potion-retrieval-32M}"
: "${MODEL_OUT:=/models/potion-retrieval-32M}"
: "${MODEL_FILES:=config.json model.safetensors modules.json tokenizer.json tokenizer_config.json vocab.txt}"

mkdir -p "$MODEL_OUT"
for f in $MODEL_FILES; do
  if [ -f "$MODEL_OUT/$f" ]; then
    echo "have $f"
  else
    echo "fetch $MODEL_REPO/$f"
    curl -fsSL "https://huggingface.co/$MODEL_REPO/resolve/main/$f" -o "$MODEL_OUT/$f"
  fi
done
echo "model ready at $MODEL_OUT"
