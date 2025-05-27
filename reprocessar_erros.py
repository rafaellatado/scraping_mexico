import pandas as pd
from main import processar_linhas

caminho_progresso = './csv/df_mx_com_telefone.csv'

def main():
    df = pd.read_csv(caminho_progresso)

    # Pega só índices das linhas com erro
    indices_erro = df.index[df['status_requisicao'] == 'erro'].tolist()

    if not indices_erro:
        print("✅ Nenhuma linha com erro para reprocessar.")
        return

    print(f"🚀 Reprocessando {len(indices_erro)} linhas com erro.")
    processar_linhas(df, indices_erro)

if __name__ == '__main__':
    main()
