import os
from tkinter import filedialog, messagebox


def save_df(df, file_type: str):

    file_object = df

    if file_type == 'xlsx':

        file_path = filedialog.asksaveasfilename(
            defaultextension=".xlsx",
            filetypes=[("Excel files", "*.xlsx")],
            title="Salvar arquivo como"
        )
        if not file_path:
            return

        file_object.to_excel(file_path, index=False)

        os.startfile(file_path)

    else:
        messagebox.showerror("Error", 'Filetype not supported, yet' )
        print('Filetype not supported, yet')

