# Reference regeneration

`make_reference.py` produces `tests/fixtures/potion-retrieval-32M.reference.json`
from the official Python implementation. The package version is part of the
oracle: model2vec 0.9 drops `[UNK]` and special ids before pooling, and that
behavior was established empirically, not from documentation — regenerating
with a different version is an oracle change and must be reviewed as one.

```sh
python3 -m venv venv && venv/bin/pip install model2vec==0.9.0 numpy
# model dir = the download_model.sh layout (tokenizer.json + model.safetensors)
cd tools && ../venv/bin/python make_reference.py
```
