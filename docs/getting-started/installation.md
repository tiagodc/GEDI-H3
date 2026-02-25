# Installation

## Prerequisites

- Python 3.12+
- conda (recommended) or pip
- NASA Earthdata account (for downloading GEDI data)

## Using conda (recommended)

```bash
git clone https://github.com/tiagodc/GEDI-H3
cd GEDI-H3

conda env create -f environment.yml -n gedih3
conda activate gedih3

gh3_build --help
```

## NASA Earthdata Credentials

GEDI data is hosted by NASA's DAAC. Authentication is required for downloads.

1. Create an account at [https://urs.earthdata.nasa.gov/](https://urs.earthdata.nasa.gov/)
2. Create `~/.netrc` with your credentials:

```
machine urs.earthdata.nasa.gov
    login YOUR_USERNAME
    password YOUR_PASSWORD
```

3. Verify authentication:

```bash
python -c "import earthaccess; earthaccess.login()"
```

## Verify Installation

```bash
gh3_build --help
gh3_list_resolutions
```
