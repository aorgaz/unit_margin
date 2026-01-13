# Unit Margin Calculation Methodology

## 1. Context and Objective
The goal is to calculate the operating margin (Energy/Power × Price) for various Spanish electricity markets at a granular level (Hourly or Quarter-hourly). The analysis targets specific Programming Units (UP) and Offer Units (UO) and requires intersecting these two sets of identifiers.

## 2. Data Sources

### 2.1 ESIOS I90 (Programming Units)
- **Source**: REE (Red Eléctrica de España)
- **Format**: ZIP files containing Excel (`.xls`).
- **Path Pattern**: `.../i90_[yyyy]/I90DIA_[yyyymmdd].zip/I90DIA_[yyyymmdd].xls`
- **Content**: Energy and Power schedules per Programing Unit.
- **Key Sheets**:
    - `I90DIA27`: Bilaterales (Energy)
    - `I90DIA26`: PDBF (Energy)
    - `I90DIA03`, `I90DIA09`: Restricciones técnicas (Energy)
    - `I90DIA01`: PDVP (Energy)
    - `I90DIA05`: Banda regulación secundaria (Power) - Requires filtering by 'Subir'/'Bajar'.
    - `I90DIA37`: aFRR (Energy) - Regulación secundaria.
    - `I90DIA07`: mFRR (Energy) - Regulación terciaria.
    - `I90DIA06`: RR (Energy) - Banda de sustitución? (Check specific mapping).
    - `I90DIA11`: RR (Price) - Note: Price in I90? Usually prices are external, but this is specified.
    - `I90DIA08`, `I90DIA10`: RTTR (Energy/Price).
    - `I90DIA02`: P48 (Energy).

### 2.2 OMIE (Offer Units)
- **Source**: OMIE (Market Operator)
- **Format**: ZIP files containing text-based versioned files (e.g., `.1`, `.v`).
- **Path Pattern**: `.../[type]/[type]_[yyyymm].zip/[type]_[yyyymmdd].[v]`
- **Markets**:
    - **PDBC** (Daily Base Operating Program): Energy (`pdbc`) and Marginal Price (`marginalpdbc`).
    - **PDVD** (Daily Viable Dispatch): Energy (`pdvd`).
    - **PIBC** (Intraday): Energy (`pibci`).
    - **MIC** (Market for Contract Information/Trades): Energy & Price (`trades`).
        - *Special Note*: Trades file contains buy/sell info. Net output = Sell - Buy.

### 2.3 ESIOS Indicators (Prices)
- **Source**: REE APIs/Public files.
- **Format**: CSV.
- **Use**: Applied to Energy/Power quantities from I90 where prices are not intrinsic to the file.
- **Key Indicators**:
    - `612` - `618`: PIBC Session Prices (1-7).
    - `634`, `2130`: Banda Sec. Prices (Switch date: 2024-11-20).
    - `682`, `683`: aFRR/mFRR Prices (Switch dates apply).
    - `676`, `677`: mFRR Prices (from 2024-12-10).

## 3. Script Logic

### 3.1 Initial Filtering
- **Units**: [GUIG, GUIB, MLTG, MLTB, SLTG, SLTB, TJEG, TJEB]
- **Time**: Filter by Year (YYYY).

### 3.2 Time Handling
- **Timezones**:
    - **Madrid Time**: Local time (CET/CEST). Source files use this.
    - **UTC+1**: Standard time without DST (winter time reference).
- **Resolution**:
    - Detect if input is Hourly or Quarter-hourly.
    - Normalize for calculation.

### 3.3 Matching Logic
- The script iterates through the specified date range.
- It loads UP data (I90) and UO data (OMIE).
- It performs a name-based merge. Only units present in both sets (or mapped) with identical names are retained.

### 3.4 Margin Calculation
- `Margin = Quantity * Price`
- If Price is missing (and not 0 by definition), result is blank/null.

## 4. Directory Structure
- `src/`: Source code.
- `data/inputs`: (Optional) Local cache of input files.
- `data/outputs`: Generated CSV files.
- `logs/`: Execution logs.
