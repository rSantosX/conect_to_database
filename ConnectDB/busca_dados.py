import psycopg2
from config_db import carrega_configuracao

def get_dados_pessoais():
    """ Retrieve data from the vendors table """
    config  = carrega_configuracao()
    try:
        with psycopg2.connect(**config) as conn:
            with conn.cursor() as cur:
                cur.execute("SELECT cntcpfcgc, cntnom, cntfismae, cntfisncm FROM cnt AS a, cntfis AS b WHERE cntid = cntfiscnt AND cntcpfcgc = '44502706817' ORDER BY cntcpfcgc LIMIT 100")
                print("The number of parts: ", cur.rowcount)
                row = cur.fetchone()

                while row is not None:
                    print(row)
                    row = cur.fetchone()

    except (Exception, psycopg2.DatabaseError) as error:
        print(error)

if __name__ == '__main__':
    get_dados_pessoais()        