# py-comment-evolution

This project analyzes Python source code comments
between two detached development eras: the 2000s and
the 2020s. It materializes 16 Python
open-source repositories
(8 per era) with Software Heritage, and then extracts
comments and docstrings and runs an empirical analysis.

The analysis is completely reproducible,
step by step. I have compiled a single command
pipeline to recreate the results.
However, the steps of the project
were thought of as singular executable scripts.

This project was the subject of my Bachelor's
thesis. The thesis is available to consult as
a pdf file in this repository.

## Installation

Install uv if not already installed:

```bash
pip install uv
```

Clone the repo:

```bash
git clone https://github.com/mikegaravani/py-comment-evolution.git
cd py-comment-evolution
```

Create the environment and install dependencies:

```bash
uv sync
```

Create an account on Software Heritage Archive and
generate an API token.

Create a `.env` file and add your Software Heritage API token:

```
SWH_TOKEN=<your-token>
```

## Usage

Run the comment extraction and analysis pipeline:

```bash
uv run pipeline.py
```

This command will replicate every step of the analysis.
In order:

- Fetch and materialization of each repository's
source code from Software Heritage
- Check to ensure everything was materialized properly
- Unpacking of embedded/compressed files in the materialized
source tree
- Repository Census with useful data for each materialized repo
- File Index with metadata for each materialized repo
- Extraction of comments with `tokenize`
- Extraction of docstrings with `ast` and `parso`
- Creation of the comment "blocks" dataset
- Addition of analysis features to the dataset
- Creation of csv files with result data
- Visualization of data with `matplotlib` charts

All the source-code from the materialized repositories
will be materialized and stored inside `data/raw/software_heritage`.

All the datasets and data will be
created and stored inside `data/processed`.

All the results, including visualization charts,
will be created and stored inside `results/`.

The pipeline automatically generates data and results for the "core" and "tests_only" file subsets.

More practical, methodological and analytical information on this project can be found in my thesis.

## Troubleshooting

Software Heritage might not have all the repositories ready for download immediately. The pipeline tries to fetch each repo, if it has to wait for more than 10 seconds for a repo, it will signal to Software Heritage that it needs that repo, it will skip it and go onto the next one.

The pipeline will stop and will return an error message that says to wait and try again later, when Software Heritage might have the missing repos ready to be fetched.

In my experience, it took Software Heritage about a week to provide the missing repos.

## Contacts

For any request or inquiry, please contact me through my contact form on my personal website at [mikegaravani.com](https://mikegaravani.com/).
Thanks :)

## License

The source code is licensed under the MIT License.
The thesis document is licensed under CC BY-NC-ND 4.0.
