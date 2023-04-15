# Project-3_Net Interest Income Predict

- Description: Base on the Bank's nature, it's history performance and the previous year Balance Sheet, this year planning disbursement (in this project, ENR is used instead), this project will provide the estimation of Net interest income (NII) of the year.
- Project's value: This project helps BOM in planning the Targetted Disbursement volumes (here ENR instead) of the year so as to achieve the NII, as well as Profit
- Data source: Public Financial Statements of the banks

Fields description
- FY, Financial Year
- NII, Net interest income of the year (Thousand Billion VND)
- Cur_TA_ENR, ENR of the year (Thousand Billion VND)
- Pre_Total_Asset, Total asset of the previous year (Thousand Billion VND)
- Pre_TA_ENR, ENR of the previous year (Thousand Billion VND)
- Pre_Liability, Liability  of the previous year  (Thousand Billion VND)
- Pre_Equity , Equity  of the previous year (Thousand Billion VND)
- Pre_Retain_earning, ER  of the previous year (Thousand Billion VND)
- Crisis, 1 if the year is impact by covid/economic crisis ect.
- Structure_change, 1 if in the year, the company changes the structure incl. M&A
- Top10,  1 if it is one of the top banks in Vietnam
- Domestic, 1 if the banks is not came from another countries
