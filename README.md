# parcel-cost-report-validator 🚚🚛

### App working videos:  
- Pass Validations:         https://drive.google.com/file/d/1j8C1UUIuhrbx96wnyqFpkW3fvW_lQXGX/view?usp=drive_link  
- Fail Several Validations: https://drive.google.com/file/d/1V1M9msUMNzNdmed-9qYlfTmPdaUZb9nF/view?usp=drive_link  
- Fail Late Fees:           https://drive.google.com/file/d/19jNvhZSE1HKfs2rNQubMG7Ggw1phY4ww/view?usp=drive_link  
## English

### Description

`Weekly Freight Cost Reports Validator` is an application created in Python using the libraries `pandas` and `numpy` primarily, to ensure that the weekly report has no issues. The application compares the current report with the previous week's report and performs the following validations:

1. **Total Charge Validation**: 
   - The difference between the total charge of the current week and that of the previous week must be less than 30%.

2. **Late Fees Validation**: 
   - It confirms that the late fees box is $0 in the second tab of the file.

3. **Invoice Validation**: 
   - Invoices from the previous week should not be included in the current week's report. All invoices from the previous week are validated, stored in a DataFrame using the `pandas` library, and each invoice in the current report is compared to ensure it is not included in the DataFrame of the previous report.

4. **GL Accounts Number Validation**: 
   - It verifies that the GL Accounts number is valid.

To ensure these validations are performed correctly, the application confirms that the reports used correspond to consecutive weeks. It validates:

- That the client matches in both reports.
- That the report dates are exactly 7 days apart (one week).
- That the carrier matches in both reports.

Finally, the app provides information about which validations passed and which did not. If all validations pass, the app renames the report to a standard format following this pattern: `Client_Carrier_'Parcel Cost Report_Division_'WE'_MONTH_DAY_YEAR'`.

If any validation fails, the app uses the mentioned naming format with the word "REVIEW" at the beginning. Example: `REVIEW-Sarnova FEDEX Parcel Cost Report ALL OTHER DIVISIONS WE01062024`.

### Requirements

- Python 3.x
- pandas
- numpy

### Installation

1. Clone this repository:
   ```bash
   git clone git@github.com:FabzDev/weekly-freight-cost-reports-validator.git


#
# Reports are not shared in this repository to adhere to the company's privacy policy
#


## Español

### Descripción

`cost-reports-validator_FR8` es una aplicación creada en Python utilizando las librerías `pandas` y `numpy` principalmente, para asegurar que el reporte semanal no tenga problemas. La aplicación compara el reporte actual con el reporte de la semana anterior y realiza las siguientes validaciones:

1. **Validación del Cargo Total**: 
   - La diferencia entre el cargo total de la semana actual y el de la semana anterior debe ser menor al 30%.

2. **Validación de Late Fees**: 
   - Se confirma que la casilla de late fees esté en $0 en la segunda pestaña del archivo.

3. **Validación de Invoices**: 
   - No se deben incluir facturas de la semana anterior en el reporte de la semana actual. Se validan todas las facturas de la semana anterior, se almacenan en un DataFrame utilizando la librería `pandas`, y se compara cada factura en el reporte actual para asegurar que no esté incluida en el DataFrame del reporte anterior.

4. **Validación del Número de Cuentas GL**: 
   - Se verifica que el número de cuentas GL sea válido.

Para garantizar que estas validaciones se realicen correctamente, la aplicación confirma que los reportes utilizados correspondan a semanas consecutivas. Se valida:

- Que el cliente coincida en ambos reportes.
- Que las fechas de los reportes estén distanciadas exactamente por 7 días (una semana).
- Que el transportista coincida en ambos reportes.

Finalmente, la aplicación proporciona información sobre las validaciones que pasaron y las que no. Si todas las validaciones pasan, la aplicación renombra el reporte a un formato estándar que sigue este patrón: `Cliente_Carrier_'Parcel Cost Report_Division_'WE'_MES_DIA_AÑO'`.

Si alguna validación falla, la aplicación utiliza el formato de nombre mencionado anteriormente con la palabra "REVIEW" al principio. Ejemplo: `REVIEW-Sarnova FEDEX Parcel Cost Report ALL OTHER DIVISIONS WE01062024`.

## Requisitos

- Python 3.x
- pandas
- numpy

## Instalación

1. Clona este repositorio:
   ```bash
   git clone git@github.com:FabzDev/weekly-freight-cost-reports-validator.git
