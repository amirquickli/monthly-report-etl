# Monthly Report Data Preparation

This repository contains scripts to prepare monthly reporting data for Tableau.  
It extracts loan and lender data from MotherDuck, generates per-lender CSV files, and merges them into a single file for reporting.

---

## ▶️ Workflow Overview

1. **Run `run_exports_query.py`** on the **first of the month**.  
   - Specify the reporting window (`START_DATE`, `END_DATE`) – by default this should cover the **last 6 months**, unless the requirement changes.  
   - The script generates **one CSV per lender** inside the `output/` folder.  

2. **Run `merge_all_lenders.py`** after the exports.  
   - This merges all per-lender files into **one consolidated file**.  
   - The merged file is written to the `result/` folder.  

3. **Tableau consumes** the file located in `result/`.  

---

## ⚙️ Environment Setup

Create a `.env` file in the project root with your MotherDuck token:

```env
MOTHERDUCK_TOKEN=your_motherduck_token_here