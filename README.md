# 🌍 Satellite-Based Atmospheric CO₂ Dataset

Source Data: https://cds.climate.copernicus.eu/datasets/satellite-carbon-dioxide?tab=overview

This dataset provides observations of atmospheric carbon dioxide (CO₂) amounts obtained from **satellite instruments** 🛰️. CO₂ is a naturally occurring **greenhouse gas (GHG)** 🌱, but human activities (e.g., fossil fuel combustion 🏭🔥, deforestation 🪓) have increased its abundance to **~420 ppm** (vs. pre-industrial 280 ppm). The annual cycle (🌳🍃 seasonal uptake/release by vegetation) is most pronounced in the northern hemisphere.

---

## 🔬 Measurement Methodology
- Satellites measure **near-infrared/infrared radiation** 🔆 reflected/emitted by Earth.
- **Absorption signatures** 📉 of CO₂ (and other gases) are analyzed in radiance data to determine column-averaged CO₂ abundance.
- **Retrieval algorithms** 💻⚙️ (software) process these signatures into CO₂ concentrations. Different algorithms have unique strengths/weaknesses ⚖️.

---

## 📂 Data Products
### Two main types:
1. **Column-averaged mixing ratios (XCO₂)** 🌐  
   - Instruments: `SCIAMACHY/ENVISAT`, `TANSO-FTS/GOSAT`, `TANSO-FTS2/GOSAT2`, `OCO-2`  
   - Formats: **Level 2** (L2: orbit tracks 🛰️) and **Level 3** (L3: gridded 🌍🔳).  
2. **Mid-tropospheric CO₂ columns** 🏔️  
   - Instruments: `IASI/Metop`, `AIRS`  

---

## 📊 Data Specifications
### **Spatial Coverage**  
- Horizontal: ~70°N to 70°S 🌍  
- Resolutions:  
  - SCIAMACHY (L2): 30x60 km²  
  - TANSO (L2): 10 km diameter  
  - IASI (L2): 12 km diameter  
  - L3 Gridded: 5°x5° #️⃣  

### **Temporal Coverage** 📅  
| Instrument      | Period                     |
|-----------------|----------------------------|
| SCIAMACHY (L2)  | Oct 2002 - Apr 2012        |
| IASI (L2)       | Jul 2007 - Dec 2021        |
| TANSO (L2)      | Apr 2009 - Dec 2021        |
| L3 Products     | Aug 2003 - Mar 2021        |

### **File Format** 📁  
- **NetCDF4** (CF-1.6 conventions ✅)  
- Updated **yearly** 🔄 (1 year behind real-time).  

---

## 📈 Key Variables  
| Variable                          | Units | Description                                  |
|-----------------------------------|-------|----------------------------------------------|
| **XCO₂** 🌐                      | ppm   | Column-average dry-air mole fraction of CO₂ |
| **Mid-tropospheric CO₂** 🏔️     | ppm   | Mid-troposphere CO₂ mixing ratio            |

---

## 🛠️ Usage Notes  
- **Latest versions** ⬆️ are recommended (multiple algorithm/file versions exist).  
- **Documentation** 📄 details sensor-specific sampling frequencies and auxiliary variables (e.g., pressure, temperature, aerosols).  

---

## 📜 Attribution  
- Produced for **C3S** (Copernicus Climate Change Service) 🌍, except SCIAMACHY/AIRS L2 (developed under ESA’s **GHG-CCI** project).  
