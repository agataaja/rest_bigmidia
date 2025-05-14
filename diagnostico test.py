import pandas as pd


df_main = pd.read_excel(r"C:\Users\agata\CBW 2024\diagnostico de federaçõpes dia 10.xlsx")

#df_main.set_index('FEDERAÇÃO', inplace = True)

print(df_main)
result_data = {
    'estado': [],
    'area': [],
    'item': [],
    'resposta': [],
    'consideração': [],
    'observações':[]
}
result_df = pd.DataFrame(result_data)

df_list = []

for index, row in df_main.iterrows():

    row_df = pd.DataFrame(row)

    for i, values in row_df.iterrows():

        valor = str(df_main.loc[index][i])

        if "*" in valor:

            print("\n",i,"\n")

            result_df = result_df._append({
                'estado': df_main.loc[index]['FEDERAÇÃO'],
                'area': "",
                'item': i,
                'resposta': valor,
                'consideração': 'evidencia não anexada/insuficiente',
                'observações': ""
            }, ignore_index=True)

            df_list.append(result_df)

            #result_df['estado'] = df_main.loc[index]['FEDERAÇÃO']
            #result_df['item'] = i
            #result_df['consideração'] = 'evidencia não anexada/insuficiente'

    print(result_df)


##(pd.concat(df_list, ignore_index=True)).drop_duplicates().to_excel(excel_writer= r"C:\Users\agata\CBW 2024\diagnostico de federaçõpes dia 100.xlsx", sheet_name="planilha")



