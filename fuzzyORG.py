import pandas as pd
import psycopg2
from fuzzywuzzy import fuzz

# Lista de nomes de organizações extraídos
organizations_extracted = [
    'ADVENTIST HEALTH SYSTEM SUNBELT INC',
    'THE JEWETT OTRHOPAEDIC CLINIC',
    'ORTHO ASSOC',
    'FLORIDA CENTER FOR ORTHOPAEDICS',
    'ORLANDO REGIONAL HEALTHCARE SYSTEMS',
    'ASTRA USA',
    'JANSSEN PHARMACEUTICALS',
    'WOLVERINE ANESTHESIA CONSULTANTS',
    'ORTHO CLINIC',
    'MARTIN MEMORIAL PHYSICIANS',
    'THREE RIVERS ORTHOPEDICS ASSOCIATES'
    'GOOD SAMARITAN HOSPITAL',
    'NEAVDA ORTHOPEDIC'
]

def buscar_no_gcp_por_organizacao(conexao, organization_name):
    query = """
    SELECT * FROM doctors.npidata 
    WHERE provider_organization_name_legal_business_name = %s
    """
    params = [organization_name]
    with conexao.cursor() as cursor:
        cursor.execute(query, params)
        colnames = [desc[0] for desc in cursor.description]
        results = [dict(zip(colnames, row)) for row in cursor.fetchall()]
    return results

def main():
    conexao = psycopg2.connect(
        dbname='providerdatasets5',
        user='postgres_read',
        password='this_is_the_password',
        host='35.202.191.18',
        port='5432'
    )

    results = []

    for organization in organizations_extracted:
        potential_matches = buscar_no_gcp_por_organizacao(conexao, organization)

        best_match = None
        best_score = 0

        for npi_record in potential_matches:
            npi_org_name = npi_record['provider_organization_name_legal_business_name']
            score = fuzz.ratio(organization, npi_org_name)
            if score > best_score:
                best_score = score
                best_match = npi_record

        # Definindo um threshold mínimo de 90 para considerar uma correspondência válida
        if best_match and best_score > 90:
            results.append({
                'Organization': organization,
                'Matched NPI': best_match['npi'],
                'Matched Organization Name': best_match['provider_organization_name_legal_business_name'],
                'Matched City': best_match['provider_business_mailing_address_city_name'],
                'Matched State': best_match['provider_business_mailing_address_state_name']
            })

    conexao.close()
    results_df = pd.DataFrame(results)
    results_df.to_excel('organization_matches2.xlsx', index=False)

if __name__ == "__main__":
    main()
