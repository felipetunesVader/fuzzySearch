import pandas as pd
import psycopg2
from fuzzywuzzy import fuzz

# Lista de nomes extraídos
names_extracted = [
    'ALISSON MCPHERSON',
    '',
    'JOHN MCCUTCHEN',
    'NICOLE GOODMAN',
    'Jeffrey Baum',
    'ELIANA KRETZMANN',
    'RICHARD SMITH',
    'Jon Donshik',
    'Andrew S Ellowitz',
    'Patrick McNulty',
    'Samson Otuwa'
]

def buscar_no_gcp_por_nome(conexao, first_name, last_name):
    query = """
    SELECT * FROM doctors.npidata 
    WHERE provider_first_name LIKE %s OR provider_last_name_legal_name LIKE %s
    """
    params = [first_name + '%', last_name + '%']
    with conexao.cursor() as cursor:
        cursor.execute(query, params)
        colnames = [desc[0] for desc in cursor.description]
        results = [dict(zip(colnames, row)) for row in cursor.fetchall()]
    return results

def extract_name_parts(defendant):
    parts = defendant.split(' ')
    if len(parts) == 1:
        return parts[0], None, None
    elif len(parts) == 2:
        return parts[0], parts[1], None
    else:
        return parts[0], parts[-1], parts[1]

def main():
    conexao = psycopg2.connect(
        dbname='providerdatasets5',
        user='postgres_read',
        password='this_is_the_password',
        host='35.202.191.18',
        port='5432'
    )

    results = []

    for defendant in names_extracted:
        first_name, last_name, middle_initial = extract_name_parts(defendant)
        if not first_name or not last_name:
            continue

        potential_matches = buscar_no_gcp_por_nome(conexao, first_name, last_name)

        best_match = None
        best_score = 0

        for npi_record in potential_matches:
            npi_name = f"{npi_record['provider_first_name']} {npi_record.get('provider_middle_name', '')} {npi_record['provider_last_name_legal_name']}".strip()
            score = fuzz.ratio(defendant, npi_name)
            if score > best_score:
                best_score = score
                best_match = npi_record

        if best_match:
            results.append({
                'Defendant': defendant,
                'Matched NPI': best_match['npi'],
                'Matched Name': f"{best_match['provider_first_name']} {best_match.get('provider_middle_name', '')} {best_match['provider_last_name_legal_name']}".strip(),
                'Matched Address': f"{best_match['provider_first_line_business_practice_location_address']} {best_match['provider_second_line_business_practice_location_address']}".strip(),
                'Matched State': best_match['provider_business_practice_location_address_state_name']
            })

    conexao.close()
    results_df = pd.DataFrame(results)
    results_df.to_excel('matches.xlsx', index=False)

if __name__ == "__main__":
    main()
