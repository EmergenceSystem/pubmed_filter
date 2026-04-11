# pubmed_filter

EmergenceSystem filter that searches PubMed for biomedical literature. Uses the NCBI E-utilities API (free, no key required).

## Input

```json
{"query": "CRISPR gene editing cancer"}
```

| Field     | Type    | Default | Description              |
|-----------|---------|---------|--------------------------|
| `query`   | string  | —       | Search term              |
| `timeout` | integer | `15`    | HTTP timeout in seconds  |

## Output

Up to 10 embryos, one per article:

```json
{
  "properties": {
    "url":    "https://pubmed.ncbi.nlm.nih.gov/12345678/",
    "resume": "Smith J, Doe A — Nature — 2023",
    "title":  "CRISPR-Cas9 therapeutic applications in oncology",
    "pmid":   "12345678",
    "source": "pubmed.ncbi.nlm.nih.gov"
  }
}
```

Uses a two-step pipeline: `esearch` (IDs) → `esummary` (metadata).

## Capabilities

`pubmed`, `medicine`, `biology`, `research`, `papers`, `ncbi`

## Usage

```bash
rebar3 shell
```

## License

Apache-2.0
