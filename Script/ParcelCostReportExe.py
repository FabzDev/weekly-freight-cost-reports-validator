import re
from datetime import datetime, timedelta
import os
import polars as pl

# pip install polars[excel]

def get_files():
    global new_file, prev_file
    global prev_total_df, prev_summary_df, prev_apdetail_df
    global new_total_df,  new_summary_df,  new_apdetail_df

    os.chdir('C:/Users/fabio/Documents/SAN/new_reports')
    prev_file_quotes = input("\nInsert PREVIOUS week file: ")
    prev_file = prev_file_quotes.strip('"')
    new_file  = "C:\\Users\\fabio\\Documents\\SAN\\Automatization\\FedEx Parcel Cost Report.xlsx"

    read_args = dict(has_header=False, infer_schema_length=0)  # todo como string

    prev_total_df    = pl.read_excel(prev_file, sheet_name='Total',     columns=[0],          **read_args)
    prev_summary_df  = pl.read_excel(prev_file, sheet_name='Summary',   columns=[1, 4],       **read_args)
    prev_apdetail_df = pl.read_excel(prev_file, sheet_name='AP Detail', columns=[1, 3, 5, 6], **read_args)

    new_total_df     = pl.read_excel(new_file,  sheet_name='Total',     columns=[0],          **read_args)
    new_summary_df   = pl.read_excel(new_file,  sheet_name='Summary',   columns=[1, 4],       **read_args)
    new_apdetail_df  = pl.read_excel(new_file,  sheet_name='AP Detail', columns=[1, 3, 5, 6], **read_args)


def get_carriers():
    global prev_carrier_name, new_carrier_name
    prev_carrier_name = prev_total_df[1, 0].split(' ')[0]
    new_carrier_name  = new_total_df[1, 0].split(' ')[0]


def get_clients():
    global prev_client_name, new_client_name, is_sarnova
    is_sarnova       = new_summary_df[0, 1] == 'SARNOVA'
    prev_client_name = name_dictionary(prev_apdetail_df[3, 0])
    new_client_name  = name_dictionary(new_apdetail_df[3, 0])


NAME_DICT = {
    'DIGITECH':                  'ALL OTHER DIVISIONS',
    'BOUNDTREE MEDICAL':         'Boundtree',
    'CARDIO PARTNERS':           'Cardio',
    'EMERGENCY MEDICAL PRODUCTS':'EMP',
    'TRI-ANIM HEALTH SERVICES':  'Tri Anim',
    'IWP':                       'IWP',
    'JME':                       'JME',
    'REPAIR CLINIC':             'Repair Clinic',
    'SUNDBERG':                  'Sundberg',
}

def name_dictionary(client_name):
    return NAME_DICT.get(client_name, client_name.capitalize())


def get_amounts():
    global prev_total_amount, new_total_amount, total_amount_diff
    prev_total_amount = float(prev_total_df[10, 0].split('$')[1].replace(',', ''))
    new_total_amount  = float(new_total_df[10, 0].split('$')[1].replace(',', ''))
    total_amount_diff = min(prev_total_amount, new_total_amount) / max(prev_total_amount, new_total_amount) * 100


def get_late_payment():
    global late_payment_amount
    col_name = new_summary_df.columns[0]
    val_name = new_summary_df.columns[1]
    row = new_summary_df.filter(pl.col(col_name) == 'Late Payment Fees')
    late_payment_amount = row[0, val_name] if len(row) > 0 else '0'


def get_dates():
    global prev_str_date, new_str_date, are_dates_correct, date_formatted, bill_date
    prev_str_date = prev_total_df[3, 0].split(' ')[3]
    prev_date     = datetime.strptime(prev_str_date, "%m/%d/%Y")

    new_str_date = new_total_df[3, 0].split(' ')[3]
    new_date     = datetime.strptime(new_str_date, "%m/%d/%Y")

    bill_date_raw     = new_apdetail_df[0, 2].split(' ')[0]
    bill_date         = datetime.strptime(bill_date_raw, "%Y-%m-%d")
    are_dates_correct = (new_date == prev_date + timedelta(days=7)) and \
                        (prev_date == bill_date - timedelta(days=1))

    m = re.match(r'^(\d+)/(\d+)/(\d+)$', new_str_date)
    mes, dia, anio = m.groups()
    date_formatted = '{:02d}{:02d}{}'.format(int(mes), int(dia), anio)


def check_dupes():
    global there_no_dupes, dupes_intersection
    inv_col = prev_apdetail_df.columns[1]   # col index 3 → 2nd selected col
    skip    = {'INVOICE NUMBER', None, ''}

    prev_invoices = set(prev_apdetail_df[inv_col].to_list()) - skip
    new_invoices  = set(new_apdetail_df[inv_col].to_list())  - skip

    dupes_intersection = new_invoices & prev_invoices
    there_no_dupes     = len(dupes_intersection) == 0


def check_glcode():
    global new_glcode_df
    inv_col = new_apdetail_df.columns[1]   # invoice  (original col 3)
    gl_col  = new_apdetail_df.columns[3]   # gl acct  (original col 6)

    new_glcode_df = new_apdetail_df.filter(
        pl.col(inv_col).is_not_null() &
        (pl.col(inv_col) != 'INVOICE NUMBER') &
        pl.col(gl_col).is_null()
    )


def final_validation():
    global client_matches, carrier_matches, amount_valid
    global late_payment_amount_valid, glcodes_valid, is_final_validation

    client_matches            = new_client_name == prev_client_name
    carrier_matches           = new_carrier_name == prev_carrier_name
    amount_valid              = (min(new_total_amount, prev_total_amount) /
                                 max(new_total_amount, prev_total_amount) * 100) > 35
    late_payment_amount_valid = late_payment_amount == '0'
    glcodes_valid             = new_glcode_df.is_empty() if is_sarnova else True

    if not late_payment_amount_valid:
        print(f'\n⚠️ Late Payment Fees: ${late_payment_amount}\n')
    if not there_no_dupes:
        print(f'⚠️ Dupes:\n {dupes_intersection}\n')
    if not glcodes_valid:
        print(f'⚠️ No GL_ACCOUNT:\n {new_glcode_df}\n')

    is_final_validation = (client_matches and carrier_matches and are_dates_correct and
                           amount_valid and late_payment_amount_valid and
                           there_no_dupes and glcodes_valid)


def change_file_name(finalValidation: bool):
    if finalValidation:
        os.remove(prev_file)
        if is_sarnova:
            os.rename(new_file, f'Sarnova {new_carrier_name.upper()} Parcel Cost Report {new_client_name} WE{date_formatted}.xlsx')
            return
        os.rename(new_file, f'{new_client_name} {new_carrier_name.upper()} Parcel Cost Report WE{date_formatted}.xlsx')
    else:
        if is_sarnova:
            os.rename(new_file, f'REVIEW - Sarnova {new_carrier_name.upper()} Parcel Cost Report {new_client_name} WE{date_formatted}.xlsx')
            return
        os.rename(new_file, f'REVIEW - {new_client_name} {new_carrier_name.upper()} Parcel Cost Report WE{date_formatted}.xlsx')


def main():
    get_files()
    get_carriers()
    get_clients()
    get_amounts()
    get_late_payment()
    get_dates()
    check_dupes()
    check_glcode()
    final_validation()
    change_file_name(is_final_validation)


if __name__ == "__main__":
    repeat = True
    while repeat:
        main()

        if is_final_validation:
            print('\n\t🎉 TASK COMPLETED SUCCESSFULLY!!')
        else:
            print(f'\n\t⛔ WARNING: THE PROCESS ENCOUNTERED AN ERROR. PLEASE CHECK VALIDATIONS!!')

        new_gl_value = glcodes_valid if is_sarnova else 'N/A'

        print(f'''
            {'✅' if client_matches            else '❌'} Client matches: {client_matches}
            {'✅' if carrier_matches           else '❌'} Carrier matches: {carrier_matches}
            {'✅' if are_dates_correct         else '❌'} Date matches: {are_dates_correct}
            {'✅' if amount_valid              else '❌'} Total Amounts validated: {amount_valid}
            {'✅' if late_payment_amount_valid  else '❌'} Late Payment fee $0: {late_payment_amount_valid}
            {'✅' if there_no_dupes            else '❌'} No duplicates: {there_no_dupes}
            {'✅' if glcodes_valid             else '❌'} GL Accounts valid: {new_gl_value}''')