# Kureeritud ERB

### Esialgne toru kirjeldus

0. **MARC21XML andmed OAI-PMH liideses**

&#8595; `src/python/harvest.py`

1. **MARC21XML andmed**: `data/raw/`

&#8595; `src/python/convert.py`

2. **Tabeliks konverteeritud andmed**: `data/converted/`

&#8595; `src/python/clean.py`

3. **Filtreeritud ja puhastatud andmed**: `data/cleaned/`

&#8595; harmoniseerimise skript(id) -> ❗ **tähtaeg 09.08**

4. **Harmoniseeritud andmed**: `data/harmonized/`

&#8595; linkimise skript(id) -> ❗ **tähtaeg 09.08**

5. **Lingitud (lõplikud?) andmed**: `data/linked/`

### Meelespea

&#9758; säilitame modulaarsust (iga asja jaoks oma funktsioon)

&#9758; dokumenteerime käigu pealt (inglise keeles)

&#9758; vaheastmetena kasutame `.parquet` faile

&#9758; olulist abiinfot hoiame `config` kaustas
