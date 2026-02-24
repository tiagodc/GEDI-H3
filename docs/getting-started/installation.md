# Installation

## Prerequisites

- Python 3.12+
- conda (recommended) or pip
- NASA Earthdata account (for downloading GEDI data)

## Using conda (recommended)

```bash
git clone https://github.com/tiagodc/gedih3
cd gedih3

conda env create -f environment.yml
conda activate gedih3

pip install -e .
```

## Using pip

```bash
pip install gedih3
```

### Documentation dependencies

```bash
pip install "gedih3[docs]"
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
gh3_list_variables -l2a
gh3_list_resolutions
```
